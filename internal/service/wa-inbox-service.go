package service

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"gorm.io/gorm"

	"g_backend/internal/models"
)

// messageHistoryLimit caps how many messages a chat's *initial* load
// returns (i.e. right when a chat is opened, afterSeq == 0 in
// ListMessages). Without a cap, chats carrying a large history-synced
// backlog would return (and the frontend would fully re-render) their
// entire history on every single poll — a major source of the UI feeling
// heavy/laggy. Later polls only ask for messages newer than the last one
// they've already seen (see ListMessages' afterSeq parameter), which is a
// small, cheap delta regardless of how long the chat's history is.
const messageHistoryLimit = 50

// WaInboxService owns chat/message history for connected WhatsApp
// devices: persisting incoming messages, listing chats and messages, and
// sending outgoing text messages through the live whatsmeow client
// managed by WaConnectDeviceService. Every operation is scoped by
// DeviceID, since one user can have several connected devices, each with
// its own independent set of chats.
//
// Only plain text is handled for now (see extractText) — media,
// reactions, etc. can be added later without changing this shape.
type WaInboxService struct {
	db      *gorm.DB
	devices *WaConnectDeviceService

	// mediaStorageRoot is the local disk folder decrypted media files
	// (sent and received) are written under — see wa-media-service.go.
	mediaStorageRoot string

	// avatarCheckedMu/avatarChecked remembers WHEN we last resolved an
	// avatar for a (deviceID, chatJID) pair, so ListMessages doesn't hit
	// MySQL on every single poll just to find out there's nothing new to
	// fetch. Unlike the old permanent boolean this replaced, a value
	// here only holds for avatarLocalCacheTTL — WhatsApp's profile
	// picture URLs are NOT actually permanent (see ensureAvatar's
	// docblock, and WaChat.AvatarCheckedAt), so this must eventually let
	// ensureAvatar re-check, not just short-circuit it forever.
	avatarCheckedMu sync.Mutex
	avatarChecked   map[string]map[string]time.Time

	// laravelBaseURL/webhookAPIKey/webhookClient back notifyIncomingMessageWebhook
	// below — Laravel's "Auto Reply (Kata Kunci)" feature (matching an
	// incoming message's text against a configured keyword and replying)
	// has no other way to find out a message arrived, since Laravel
	// itself has no live WhatsApp connection.
	laravelBaseURL string
	webhookAPIKey  string
	webhookClient  *http.Client
}

func NewWaInboxService(db *gorm.DB, devices *WaConnectDeviceService, mediaStorageRoot, laravelBaseURL, webhookAPIKey string) *WaInboxService {
	return &WaInboxService{
		db:               db,
		devices:          devices,
		mediaStorageRoot: mediaStorageRoot,
		avatarChecked:    make(map[string]map[string]time.Time),
		laravelBaseURL:   laravelBaseURL,
		webhookAPIKey:    webhookAPIKey,
		webhookClient:    &http.Client{Timeout: 5 * time.Second},
	}
}

// deviceOwner looks up which user owns a device. Used by the event-driven
// entry points (SaveIncomingMessage/HandleHistorySync), which only ever
// receive a deviceID from whatsmeow's event handler — not a userID from
// an authenticated request.
func (s *WaInboxService) deviceOwner(deviceID string) (string, bool) {
	var device models.WaDevice
	if err := s.db.Select("user_id").Where("id = ?", deviceID).First(&device).Error; err != nil {
		return "", false
	}
	return device.UserID, true
}

// SaveIncomingMessage persists a message delivered over whatsmeow's event
// handler and refreshes the owning chat's summary. This implements
// WaConnectDeviceService's MessageStore interface.
func (s *WaInboxService) SaveIncomingMessage(deviceID string, evt *events.Message) {
	log.Printf("wa-inbox: SaveIncomingMessage: event received (device=%s from=%s fromMe=%v msgID=%s)", deviceID, evt.Info.Sender.String(), evt.Info.IsFromMe, evt.Info.ID)

	userID, ok := s.deviceOwner(deviceID)
	if !ok {
		log.Printf("wa-inbox: SaveIncomingMessage: no owner found for device %s — message dropped, no auto-reply possible", deviceID)
		return
	}

	// Media (image/video/audio/document/sticker) needs its own path —
	// downloading + writing the decrypted file to disk — handled
	// entirely in wa-media-service.go's saveIncomingMedia. Anything not
	// recognized as one of those five falls through to the plain-text
	// handling below as before.
	if media := detectMediaMessage(evt.Message); media != nil {
		log.Printf("wa-inbox: SaveIncomingMessage: message is media, not text — auto-reply webhook not applicable (device=%s)", deviceID)
		s.saveIncomingMedia(deviceID, userID, evt, media)
		return
	}

	// A poll vote arrives as its own message type (PollUpdateMessage),
	// carrying no text body at all — without this branch it would simply
	// fall through extractText() below and be silently dropped as "no
	// extractable text body". See handlePollVote's docblock for the
	// decrypt-and-match flow.
	if evt.Message.GetPollUpdateMessage() != nil {
		s.handlePollVote(deviceID, evt)
		return
	}

	body := extractText(evt.Message)
	if body == "" {
		log.Printf("wa-inbox: SaveIncomingMessage: no extractable text body (device=%s) — skipped, no auto-reply webhook fired", deviceID)
		return // skip anything that's neither text nor a downloadable media type (reactions, etc.)
	}

	chatJID := evt.Info.Chat.String()

	client, _ := s.devices.GetClient(deviceID)
	name := s.resolveChatName(context.Background(), deviceID, client, evt.Info.Chat, evt.Info.PushName)
	senderPhone := s.resolveSenderPhone(deviceID, client, chatJID)

	s.upsertChat(userID, deviceID, chatJID, name, body, evt.Info.Timestamp, !evt.Info.IsFromMe)

	s.saveMessageOnce(deviceID, chatJID, &models.WaMessage{
		UserID:      userID,
		DeviceID:    deviceID,
		ChatJID:     chatJID,
		MessageID:   string(evt.Info.ID),
		SenderJID:   evt.Info.Sender.String(),
		FromMe:      evt.Info.IsFromMe,
		Body:        body,
		MessageType: models.WaMessageTypeText,
		SentAt:      evt.Info.Timestamp,
	})

	// Only genuinely incoming messages (not the device's own sent
	// messages echoed back by WhatsApp) should ever be able to trigger
	// an auto-reply — otherwise a reply Laravel sends could loop back
	// through this same handler and trigger another reply.
	if !evt.Info.IsFromMe {
		log.Printf("wa-inbox: SaveIncomingMessage: dispatching incoming-message webhook (device=%s chat=%s bodyLen=%d)", deviceID, chatJID, len(body))
		s.notifyIncomingMessageWebhook(deviceID, userID, chatJID, senderPhone, string(evt.Info.ID), body, evt.Info.Timestamp)
	} else {
		log.Printf("wa-inbox: SaveIncomingMessage: message is from own device (fromMe) — auto-reply webhook intentionally skipped to avoid reply loops (device=%s)", deviceID)
	}
}

// resolveSenderPhone returns the sender's real phone number for chatJID
// (digits only, no "+"), resolving WhatsApp's "@lid" (Linked ID)
// addressing through the device's own LID<->phone-number store when
// needed. This is the same lookup ensurePhone uses to lazily cache
// wa_chats.phone, but done live/synchronously here so a real number is
// available at the exact moment the incoming-message webhook fires —
// ensurePhone's cache is only populated the first time a chat is opened
// in the inbox UI, which may well be *after* this message (e.g. the very
// first message in a brand new chat, or a device whose inbox nobody has
// opened yet).
//
// Returns "" if it can't be resolved (group/channel JIDs, or a LID
// WhatsApp hasn't told this device the phone-number counterpart for
// yet) — Laravel treats a blank/missing sender_phone as "fall back to
// parsing chat_jid the old way", so this never blocks the webhook from
// firing, it just loses the phone-based Jadwal confirmation matching for
// that one message.
func (s *WaInboxService) resolveSenderPhone(deviceID string, client *whatsmeow.Client, chatJID string) string {
	jid, err := types.ParseJID(chatJID)
	if err != nil || jid.Server == types.GroupServer || jid.Server == "newsletter" {
		return ""
	}

	if jid.Server != "lid" {
		return jid.User
	}

	if client == nil || client.Store == nil || client.Store.LIDs == nil {
		return ""
	}

	pn, err := client.Store.LIDs.GetPNForLID(context.Background(), jid)
	if err != nil || pn.User == "" {
		return ""
	}

	return pn.User
}

// notifyIncomingMessageWebhook tells Laravel a text message just arrived,
// for its "Auto Reply (Kata Kunci)" feature to act on (matching `body`
// against configured keywords and replying). Fire-and-forget in its own
// goroutine: a slow or unreachable Laravel must never delay/break
// whatsmeow's own event processing, which is why this isn't called
// synchronously inline in SaveIncomingMessage. Best-effort — if it fails,
// the message is still saved normally above, just no auto-reply fires for
// it; there's no retry queue on this side.
func (s *WaInboxService) notifyIncomingMessageWebhook(deviceID, userID, chatJID, senderPhone, messageID, body string, sentAt time.Time) {
	if s.laravelBaseURL == "" {
		return
	}

	payload, err := json.Marshal(map[string]any{
		"device_id":    deviceID,
		"user_id":      userID,
		"chat_jid":     chatJID,
		"sender_phone": senderPhone,
		"message_id":   messageID,
		"body":         body,
		"sent_at":      sentAt.UTC().Format(time.RFC3339),
	})
	if err != nil {
		log.Printf("wa-inbox: failed to marshal incoming-message webhook payload: %v", err)
		return
	}

	log.Printf("wa-inbox: notifyIncomingMessageWebhook: POSTing to %s/api/webhooks/wa/incoming-message (device=%s chat=%s messageID=%s)", s.laravelBaseURL, deviceID, chatJID, messageID)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("wa-inbox: panic recovered in notifyIncomingMessageWebhook goroutine (device=%s messageID=%s): %v", deviceID, messageID, r)
			}
		}()

		req, err := http.NewRequest(http.MethodPost, s.laravelBaseURL+"/api/webhooks/wa/incoming-message", bytes.NewReader(payload))
		if err != nil {
			log.Printf("wa-inbox: failed to build incoming-message webhook request: %v", err)
			return
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-API-KEY", s.webhookAPIKey)

		resp, err := s.webhookClient.Do(req)
		if err != nil {
			log.Printf("wa-inbox: incoming-message webhook to Laravel failed: %v", err)
			return
		}
		defer resp.Body.Close()

		bodyBytes, _ := io.ReadAll(resp.Body)

		if resp.StatusCode >= 300 {
			log.Printf("wa-inbox: incoming-message webhook to Laravel returned status %d: %s", resp.StatusCode, string(bodyBytes))
			return
		}

		log.Printf("wa-inbox: incoming-message webhook delivered OK (status %d): %s", resp.StatusCode, string(bodyBytes))
	}()
}

// notifyMessageStatusWebhook tells Laravel a sent message's delivery/read
// status just advanced — same fire-and-forget, best-effort shape as
// notifyIncomingMessageWebhook above (a slow/unreachable Laravel must
// never delay whatsmeow's own event processing, and there's no retry
// queue on this side: a missed receipt just leaves that one
// wa_message_schedule_logs row one status behind, not wrong).
func (s *WaInboxService) notifyMessageStatusWebhook(deviceID, messageID, status string, deliveredCount, readCount, recipientTotal int) {
	if s.laravelBaseURL == "" {
		return
	}

	payload, err := json.Marshal(map[string]any{
		"device_id":       deviceID,
		"message_id":      messageID,
		"status":          status,
		"delivered_count": deliveredCount,
		"read_count":      readCount,
		"recipient_total": recipientTotal,
	})
	if err != nil {
		log.Printf("wa-inbox: failed to marshal message-status webhook payload: %v", err)
		return
	}

	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("wa-inbox: panic recovered in notifyMessageStatusWebhook goroutine (device=%s messageID=%s): %v", deviceID, messageID, r)
			}
		}()

		req, err := http.NewRequest(http.MethodPost, s.laravelBaseURL+"/api/webhooks/wa/message-status", bytes.NewReader(payload))
		if err != nil {
			log.Printf("wa-inbox: failed to build message-status webhook request: %v", err)
			return
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-API-KEY", s.webhookAPIKey)

		resp, err := s.webhookClient.Do(req)
		if err != nil {
			log.Printf("wa-inbox: message-status webhook to Laravel failed: %v", err)
			return
		}
		defer resp.Body.Close()

		if resp.StatusCode >= 300 {
			bodyBytes, _ := io.ReadAll(resp.Body)
			log.Printf("wa-inbox: message-status webhook to Laravel returned status %d: %s", resp.StatusCode, string(bodyBytes))
		}
	}()
}

// HandleHistorySync ingests the batch of past conversations/messages
// WhatsApp sends shortly after a device is linked (or periodically
// resyncs). Without this, only messages that arrive *after* connecting
// would ever show up — existing chats would look empty. This implements
// WaConnectDeviceService's MessageStore interface.
func (s *WaInboxService) HandleHistorySync(deviceID string, evt *events.HistorySync) {
	if evt == nil || evt.Data == nil {
		return
	}

	userID, ok := s.deviceOwner(deviceID)
	if !ok {
		return
	}

	for _, conv := range evt.Data.GetConversations() {
		chatJID := conv.GetID()
		if chatJID == "" {
			continue
		}

		name := conv.GetDisplayName()
		if name == "" {
			name = conv.GetName()
		}
		if name == "" {
			if jid, err := types.ParseJID(chatJID); err == nil {
				client, _ := s.devices.GetClient(deviceID)
				if jid.Server == types.GroupServer {
					name = s.groupName(context.Background(), deviceID, client, jid)
				} else if pnJID := conv.GetPnJID(); pnJID != "" {
					// WhatsApp includes the phone-number JID counterpart
					// directly on the conversation for "@lid" chats — no
					// live lookup needed, unlike the live-message path.
					if pn, err := types.ParseJID(pnJID); err == nil && pn.User != "" {
						name = "+" + pn.User
					}
				}
			}
		}

		var lastBody string
		var lastAt time.Time

		for _, hsMsg := range conv.GetMessages() {
			webMsg := hsMsg.GetMessage()
			if webMsg == nil {
				continue
			}

			body := extractText(webMsg.GetMessage())
			if body == "" {
				continue // skip non-text history messages for now
			}

			key := webMsg.GetKey()
			sentAt := time.Unix(int64(webMsg.GetMessageTimestamp()), 0)

			s.saveMessageOnce(deviceID, chatJID, &models.WaMessage{
				UserID:    userID,
				DeviceID:  deviceID,
				ChatJID:   chatJID,
				MessageID: key.GetID(),
				SenderJID: key.GetParticipant(),
				FromMe:    key.GetFromMe(),
				Body:      body,
				SentAt:    sentAt,
			})

			if sentAt.After(lastAt) {
				lastAt = sentAt
				lastBody = body
			}
		}

		if lastBody != "" {
			// History is never counted as unread — the user has already
			// seen these messages on their phone.
			s.upsertChat(userID, deviceID, chatJID, name, lastBody, lastAt, false)
		}
	}
}

// saveMessageOnce inserts a message unless one with the same WhatsApp
// message ID is already stored for this device+chat, so re-delivered
// history syncs (and any other duplicate delivery) don't pile up
// duplicate rows.
//
// Takes msg by pointer (not value) so GORM's Create can write the
// database-assigned ID (BeforeCreate's UUID) and Seq (the column's own
// AUTO_INCREMENT) back onto the caller's own struct. SendMessage relies
// on this: its response to the frontend carries Seq, which the frontend
// then uses as the polling cursor (?after_seq=) — if it never made it
// back (as it didn't when this took msg by value), the newly sent
// message would get re-fetched and appended a second time on the very
// next poll.
func (s *WaInboxService) saveMessageOnce(deviceID string, chatJID string, msg *models.WaMessage) {
	if msg.MessageID != "" {
		var existing models.WaMessage
		err := s.db.
			Where(models.WaMessage{DeviceID: deviceID, ChatJID: chatJID, MessageID: msg.MessageID}).
			First(&existing).Error
		if err == nil {
			msg.ID = existing.ID   // already have it — still hand back its real ID
			msg.Seq = existing.Seq // and its real ordering cursor
			return
		}
	}

	s.db.Create(msg)
}

// ListChats returns one device's conversations, most recently active
// first.
func (s *WaInboxService) ListChats(userID string, deviceID string) ([]models.WaChat, error) {
	if err := s.devices.AssertOwnership(userID, deviceID); err != nil {
		return nil, err
	}

	var chats []models.WaChat
	err := s.db.
		Where("device_id = ?", deviceID).
		Order("last_message_at DESC").
		Find(&chats).Error
	if err != nil {
		return nil, err
	}

	// Fire-and-forget: previously a chat's avatar was only ever fetched
	// once you opened it (see ensureAvatar, called from ListMessages),
	// which is why the sidebar showed blank/initial-letter avatars for
	// every chat you hadn't clicked into yet — "foto profile blm tampil
	// semuanya". The frontend polls this endpoint every 6s, so a small
	// capped batch per call gradually backfills the rest without a
	// slow response now or hammering WhatsApp with hundreds of requests
	// at once.
	go s.backfillAvatars(deviceID, chats)

	return chats, nil
}

func (s *WaInboxService) backfillAvatars(deviceID string, chats []models.WaChat) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("wa-inbox: backfillAvatars: panic recovered for device %s: %v", deviceID, r)
		}
	}()

	const maxPerCall = 15
	fetched := 0
	for _, chat := range chats {
		if fetched >= maxPerCall {
			return
		}
		// Used to skip outright whenever chat.AvatarURL was already
		// non-empty — which is exactly why a sidebar full of avatars
		// that backfilled in fine once could later go blank for every
		// contact at the same time (22 September 2026 report):
		// WhatsApp's profile-picture URLs expire, and this loop never
		// got a chance to notice or refresh an already-cached one.
		// ensureAvatar itself now owns the actual staleness check
		// (WaChat.AvatarCheckedAt vs avatarRefreshInterval); the
		// avatarCheckedRecently() call here is purely the cheap
		// in-memory fast-path so this 6s-polled backfill doesn't issue
		// a MySQL query for a chat it (or ensureAvatar, called for the
		// same chat via ListMessages) already looked at moments ago.
		if s.avatarCheckedRecently(deviceID, chat.ChatJID) {
			continue
		}
		s.ensureAvatar(deviceID, chat.ChatJID)
		fetched++
	}
}

// ListMessages returns a chat's message history.
//
//   - On the initial load (afterSeq == 0) it returns only the most recent
//     messageHistoryLimit messages, oldest first, and triggers the
//     one-time side effects of opening a chat: marking it read,
//     subscribing to presence, and fetching the contact's avatar.
//   - On subsequent polls (afterSeq > 0) it returns only messages with a
//     higher Seq than that — a small delta the frontend can append,
//     instead of re-fetching (and re-rendering) the whole thread every
//     few seconds. Seq (not ID) is what this cursor is built on: ID is a
//     random UUID with no natural order, while Seq is a plain
//     AUTO_INCREMENT column kept purely for this kind of ordering — see
//     models.WaMessage.
func (s *WaInboxService) ListMessages(userID string, deviceID string, chatJID string, afterSeq uint64) ([]models.WaMessage, error) {
	if err := s.devices.AssertOwnership(userID, deviceID); err != nil {
		return nil, err
	}

	var messages []models.WaMessage

	if afterSeq > 0 {
		err := s.db.
			Where("device_id = ? AND chat_jid = ? AND seq > ?", deviceID, chatJID, afterSeq).
			Order("seq ASC").
			Find(&messages).Error
		s.attachMediaURLs(deviceID, messages)
		return messages, err
	}

	err := s.db.
		Where("device_id = ? AND chat_jid = ?", deviceID, chatJID).
		Order("seq DESC").
		Limit(messageHistoryLimit).
		Find(&messages).Error
	if err != nil {
		return nil, err
	}

	// Fetched newest-first to apply the LIMIT to the *most recent*
	// messages; flip back to chronological (oldest first) for display.
	for i, j := 0, len(messages)-1; i < j; i, j = i+1, j-1 {
		messages[i], messages[j] = messages[j], messages[i]
	}

	s.db.Model(&models.WaChat{}).
		Where("device_id = ? AND chat_jid = ? AND unread_count > 0", deviceID, chatJID).
		Update("unread_count", 0)

	// Best-effort niceties: neither failure should break loading messages.
	s.devices.EnsurePresenceSubscription(deviceID, chatJID)
	s.ensureAvatar(deviceID, chatJID)
	s.ensurePhone(deviceID, chatJID)
	s.markIncomingAsRead(deviceID, chatJID, messages)

	s.attachMediaURLs(deviceID, messages)
	return messages, nil
}

// markIncomingAsRead tells WhatsApp (and whoever sent them) that the
// not-from-me messages in this batch have now been seen — the other
// side's own tick color depends on this; without it, a message you
// sent to this device would stay stuck on a single/double grey tick on
// THEIR end forever, the exact same bug this whole feature is fixing on
// ours. Best-effort: opening a chat must never fail just because this
// did. Skipped for group chats — a correct read receipt there needs
// each message's own sender JID, which this bulk "just opened the
// chat" path doesn't track; 1:1 chats don't have that wrinkle, since
// the chat's JID and the sender's JID are the same thing.
func (s *WaInboxService) markIncomingAsRead(deviceID string, chatJID string, messages []models.WaMessage) {
	jid, err := types.ParseJID(chatJID)
	if err != nil || jid.Server == types.GroupServer {
		return
	}

	var unreadIDs []types.MessageID
	for _, m := range messages {
		if !m.FromMe && m.MessageID != "" {
			unreadIDs = append(unreadIDs, types.MessageID(m.MessageID))
		}
	}
	if len(unreadIDs) == 0 {
		return
	}

	client, ok := s.devices.GetClient(deviceID)
	if !ok || client == nil {
		return
	}

	_ = client.MarkRead(context.Background(), unreadIDs, time.Now(), jid, jid)
}

// Presence returns the current online/typing state for one chat contact.
func (s *WaInboxService) Presence(userID string, deviceID string, chatJID string) (PresenceInfo, error) {
	if err := s.devices.AssertOwnership(userID, deviceID); err != nil {
		return PresenceInfo{}, err
	}
	return s.devices.GetPresence(deviceID, chatJID), nil
}

// avatarRefreshInterval -- how long a fetched avatar_url (and a
// confirmed "no picture set" outcome alike) is trusted before
// ensureAvatar asks WhatsApp again. This used to be "forever" under the
// old (incorrect) assumption that profile-picture URLs never expire —
// they do, and once enough of them expired around the same time (most
// were originally cached in the same early burst of chat opens), EVERY
// contact's avatar stopped rendering at once, which is exactly the "used
// to show, now nothing shows, all of them" symptom reported 22 September
// 2026. 24h balances staying fresh against not hammering WhatsApp's
// profile-picture endpoint on every chat open.
const avatarRefreshInterval = 24 * time.Hour

// ensureAvatar fetches and caches a contact's profile picture URL,
// re-checking once avatarRefreshInterval has passed since the last check
// (WaChat.AvatarCheckedAt) rather than trusting a cached URL forever —
// see avatarRefreshInterval's docblock for why that matters. The
// in-memory avatarChecked map is just a short-lived (avatarRefreshInterval)
// local cache on top of that DB timestamp, so repeated polling of the
// same open chat doesn't re-query MySQL every single time either.
func (s *WaInboxService) ensureAvatar(deviceID string, chatJID string) {
	if s.avatarCheckedRecently(deviceID, chatJID) {
		return
	}
	defer s.markAvatarChecked(deviceID, chatJID)

	var chat models.WaChat
	err := s.db.Where(models.WaChat{DeviceID: deviceID, ChatJID: chatJID}).First(&chat).Error
	if err == nil && chat.AvatarURL != "" && chat.AvatarCheckedAt != nil && time.Since(*chat.AvatarCheckedAt) < avatarRefreshInterval {
		return
	}

	client, ok := s.devices.GetClient(deviceID)
	if !ok || client == nil {
		return
	}

	jid, parseErr := types.ParseJID(chatJID)
	if parseErr != nil {
		return
	}

	now := time.Now()
	info, fetchErr := client.GetProfilePictureInfo(context.Background(), jid, nil)
	if fetchErr != nil || info == nil || info.URL == "" {
		// No picture set, or WhatsApp declined the request — not fatal,
		// but still record that we checked JUST NOW, so a contact with
		// genuinely no photo doesn't get re-queried on every single poll
		// either — only retried again after avatarRefreshInterval, same
		// as a successful fetch.
		s.db.Model(&models.WaChat{}).
			Where(models.WaChat{DeviceID: deviceID, ChatJID: chatJID}).
			Update("avatar_checked_at", now)
		return
	}

	s.db.Model(&models.WaChat{}).
		Where(models.WaChat{DeviceID: deviceID, ChatJID: chatJID}).
		Updates(map[string]interface{}{
			"avatar_url":        info.URL,
			"avatar_checked_at": now,
		})
}

// ensurePhone resolves and caches a chat's real phone number the first
// time its chat is opened — mirrors ensureAvatar's lazy-fetch-once
// pattern just above (find once, cache in wa_chats, never re-check).
// For ordinary chats the phone number is already sitting right in the
// JID (jid.User) and needs no network call; only "@lid" chats
// (WhatsApp's privacy-preserving linked IDs) need an actual resolution
// through WhatsApp's own LID<->phone mapping — same distinction
// displayNameFallback above already makes for the chat's *name*, this
// is the same idea for its phone number. Groups and channels have no
// phone number at all, so this is a no-op for those JID types.
func (s *WaInboxService) ensurePhone(deviceID string, chatJID string) {
	jid, err := types.ParseJID(chatJID)
	if err != nil || jid.Server == types.GroupServer || jid.Server == "newsletter" {
		return
	}

	var chat models.WaChat
	if err := s.db.Where(models.WaChat{DeviceID: deviceID, ChatJID: chatJID}).First(&chat).Error; err != nil || chat.Phone != "" {
		return // already resolved, or the chat row doesn't exist yet
	}

	if jid.Server != "lid" {
		if jid.User == "" {
			return
		}
		s.db.Model(&models.WaChat{}).
			Where(models.WaChat{DeviceID: deviceID, ChatJID: chatJID}).
			Update("phone", "+"+jid.User)
		return
	}

	client, ok := s.devices.GetClient(deviceID)
	if !ok || client == nil || client.Store == nil || client.Store.LIDs == nil {
		return
	}

	pn, err := client.Store.LIDs.GetPNForLID(context.Background(), jid)
	if err != nil || pn.User == "" {
		return // couldn't resolve — left blank rather than showing noise
	}

	s.db.Model(&models.WaChat{}).
		Where(models.WaChat{DeviceID: deviceID, ChatJID: chatJID}).
		Update("phone", "+"+pn.User)
}

// UpdateMessageStatus advances the delivery/read status of our own sent
// messages as *events.Receipt events arrive from whatsmeow — this is
// what makes the tick progression in the frontend (see ackIcon() in
// resources/views/chat/inbox/inbox.blade.php) actually reflect reality
// instead of every sent message staying frozen on a single grey tick
// forever. Implements WaConnectDeviceService's MessageStore interface.
func (s *WaInboxService) UpdateMessageStatus(deviceID string, evt *events.Receipt) {
	if evt == nil || len(evt.MessageIDs) == 0 {
		return
	}

	status := receiptStatus(evt.Type)
	if status == "" {
		return // a receipt type we don't map to a tick (e.g. "sender", "retry")
	}

	// evt.Sender is the ONE participant this specific receipt is from —
	// the group member who just delivered/read it, or the chat partner
	// themselves for a 1:1 chat (Sender == Chat there). WhatsApp sends a
	// separate *events.Receipt per group member, never one combined
	// receipt for the whole group — see WaMessageReceipt's docblock for
	// why that matters (a single WaMessage.Status column can only ever
	// record "has ANYONE reached this far", never "how many members").
	participantJID := evt.Sender.String()

	for _, id := range evt.MessageIDs {
		var msg models.WaMessage
		err := s.db.
			Where(models.WaMessage{DeviceID: deviceID, ChatJID: evt.Chat.String(), MessageID: string(id), FromMe: true}).
			First(&msg).Error
		if err != nil {
			continue // not one of ours, or the receipt raced the message insert
		}

		// Per-participant ratchet — upserts THIS participant's own
		// progression, independent of every other participant's, so a
		// second/third/... group member's receipt keeps being counted
		// even after the message-wide status below has already maxed
		// out from the first member who read it.
		var receipt models.WaMessageReceipt
		receiptErr := s.db.
			Where(models.WaMessageReceipt{WaMessageID: msg.ID, ParticipantJID: participantJID}).
			First(&receipt).Error

		if receiptErr != nil {
			s.db.Create(&models.WaMessageReceipt{
				WaMessageID:    msg.ID,
				ParticipantJID: participantJID,
				Status:         status,
			})
		} else if messageStatusRank(status) > messageStatusRank(receipt.Status) {
			s.db.Model(&models.WaMessageReceipt{}).Where("id = ?", receipt.ID).Update("status", status)
		}

		// Message-wide ratchet (unchanged from before) — "has ANY
		// participant reached this far", still what drives the single
		// ✓✓ tick icon in the inbox UI (ackIcon() in inbox.blade.php).
		if messageStatusRank(status) > messageStatusRank(msg.Status) {
			s.db.Model(&models.WaMessage{}).Where("id = ?", msg.ID).Update("status", status)
		}

		deliveredCount, readCount := s.aggregateReceiptCounts(msg.ID)
		recipientTotal := s.recipientTotalFor(deviceID, evt.Chat)

		// Tells Laravel's "Pesan Terjadwal" feature about this same
		// delivered/read progression, so the Delivered/Read columns on
		// its index/history pages (App\Http\Controllers\Chat\
		// MessageScheduleController) reflect reality — including, for a
		// group recipient, an actual "x dari y dibaca" count instead of
		// a binary yes/no — see App\Http\Controllers\Api\
		// WaMessageStatusWebhookController on the Laravel side. Only
		// scheduled sends actually have a matching wa_message_schedule_logs
		// row for this message_id; a manual inbox send just gets a no-op
		// there, which is fine. Notified on every receipt (not just ones
		// that advance msg.Status), since a second/third group member's
		// receipt should still grow the counts even when the message-wide
		// status itself already maxed out.
		s.notifyMessageStatusWebhook(deviceID, string(id), status, deliveredCount, readCount, recipientTotal)
	}
}

// aggregateReceiptCounts counts how many DISTINCT participants of a sent
// message have reached at least "delivered" / at least "read" so far —
// the actual numbers behind the "x/y dibaca" Laravel shows for a group
// recipient. For a 1:1 chat this naturally collapses to 0 or 1, same as
// the old single-status behavior, so nothing regresses for phone/user
// recipients.
func (s *WaInboxService) aggregateReceiptCounts(waMessageID string) (delivered, read int) {
	var deliveredCount int64
	s.db.Model(&models.WaMessageReceipt{}).
		Where("wa_message_id = ? AND status IN ?", waMessageID, []string{
			models.WaMessageStatusDelivered, models.WaMessageStatusRead, models.WaMessageStatusPlayed,
		}).
		Count(&deliveredCount)

	var readCount int64
	s.db.Model(&models.WaMessageReceipt{}).
		Where("wa_message_id = ? AND status IN ?", waMessageID, []string{
			models.WaMessageStatusRead, models.WaMessageStatusPlayed,
		}).
		Count(&readCount)

	return int(deliveredCount), int(readCount)
}

// recipientTotalFor returns how many people a sent message's receipts
// could possibly come from: 1 for an individual chat (there's only ever
// one recipient), or a group's cached member count (see
// WaChat.ParticipantCount, refreshed here via groupParticipantCount) —
// 0 if that's a group whose size isn't known yet, which Laravel treats
// as "denominator unknown, just show the count" rather than "0 members".
func (s *WaInboxService) recipientTotalFor(deviceID string, chat types.JID) int {
	if chat.Server != types.GroupServer {
		return 1
	}

	client, _ := s.devices.GetClient(deviceID)
	return s.groupParticipantCount(context.Background(), deviceID, client, chat)
}

// groupParticipantCount returns a group's member count, preferring
// whatever's already cached on its WaChat row (group membership rarely
// changes and GetGroupInfo is a network round trip) and only calling out
// to WhatsApp — then caching the result — when nothing's cached yet.
// Mirrors groupName's own caching pattern just above.
func (s *WaInboxService) groupParticipantCount(ctx context.Context, deviceID string, client *whatsmeow.Client, groupJID types.JID) int {
	var existing models.WaChat
	if err := s.db.Where(models.WaChat{DeviceID: deviceID, ChatJID: groupJID.String()}).First(&existing).Error; err == nil && existing.ParticipantCount > 0 {
		return existing.ParticipantCount
	}

	if client == nil {
		return 0
	}

	info, err := client.GetGroupInfo(ctx, groupJID)
	if err != nil || info == nil {
		return 0
	}

	count := len(info.Participants)
	if count > 0 {
		s.db.Model(&models.WaChat{}).
			Where(models.WaChat{DeviceID: deviceID, ChatJID: groupJID.String()}).
			Update("participant_count", count)
	}

	return count
}

// receiptStatus maps whatsmeow's receipt type to one of our own
// WaMessageStatus* constants. types.ReceiptTypeDelivered is WhatsApp's
// own zero-value convention for "plain delivery, nothing special to
// report" — not a missing/unrecognized type.
func receiptStatus(t types.ReceiptType) string {
	switch t {
	case types.ReceiptTypeDelivered:
		return models.WaMessageStatusDelivered
	case types.ReceiptTypeRead, types.ReceiptTypeReadSelf:
		return models.WaMessageStatusRead
	case types.ReceiptTypePlayed:
		return models.WaMessageStatusPlayed
	default:
		return ""
	}
}

// messageStatusRank orders WaMessage.Status weakest to strongest so
// UpdateMessageStatus can refuse to move a message's status backwards.
func messageStatusRank(status string) int {
	switch status {
	case models.WaMessageStatusSent:
		return 1
	case models.WaMessageStatusDelivered:
		return 2
	case models.WaMessageStatusRead:
		return 3
	case models.WaMessageStatusPlayed:
		return 4
	default:
		return 0
	}
}

// avatarCheckedRecently reports whether ensureAvatar has already run for
// this (deviceID, chatJID) within avatarRefreshInterval — a short-lived
// local mirror of WaChat.AvatarCheckedAt, purely to spare MySQL a query
// on every poll of an already-open chat. NOT a permanent "done forever"
// flag like the old boolean version was — that's exactly what let stale
// avatar_url values go unrefreshed indefinitely.
func (s *WaInboxService) avatarCheckedRecently(deviceID, chatJID string) bool {
	s.avatarCheckedMu.Lock()
	defer s.avatarCheckedMu.Unlock()
	checkedAt, ok := s.avatarChecked[deviceID][chatJID]
	return ok && time.Since(checkedAt) < avatarRefreshInterval
}

func (s *WaInboxService) markAvatarChecked(deviceID, chatJID string) {
	s.avatarCheckedMu.Lock()
	defer s.avatarCheckedMu.Unlock()
	if s.avatarChecked[deviceID] == nil {
		s.avatarChecked[deviceID] = make(map[string]time.Time)
	}
	s.avatarChecked[deviceID][chatJID] = time.Now()
}

// SendMessage sends a text message through one of the user's connected
// devices and records it as an outgoing message.
func (s *WaInboxService) SendMessage(ctx context.Context, userID string, deviceID string, chatJID, body string) (*models.WaMessage, error) {
	if err := s.devices.AssertOwnership(userID, deviceID); err != nil {
		return nil, err
	}

	client, err := s.devices.EnsureConnectedClient(deviceID)
	if err != nil {
		return nil, err
	}

	jid, err := types.ParseJID(chatJID)
	if err != nil {
		return nil, fmt.Errorf("wa: invalid chat id: %w", err)
	}

	// Anti-ban backstop: only one outbound send may be in flight for this
	// device at a time — see WaConnectDeviceService's sendSlots docblock.
	// Acquired right before the network call, released the instant it
	// returns (success or failure), so this manual/API send can never
	// physically overlap with a broadcast recipient or an AI Bot/
	// auto-reply going out on the same device at the same instant.
	release, err := s.devices.AcquireSendSlot(ctx, deviceID)
	if err != nil {
		return nil, fmt.Errorf("wa: timed out waiting to send: %w", err)
	}
	defer release()

	proto := &waE2E.Message{Conversation: &body}
	resp, err := client.SendMessage(ctx, jid, proto)
	if err != nil {
		return nil, fmt.Errorf("wa: failed to send message: %w", err)
	}

	now := time.Now()
	name := s.resolveChatName(ctx, deviceID, client, jid, "")
	s.upsertChat(userID, deviceID, chatJID, name, body, now, false)

	// MessageID is set from WhatsApp's own response so that if this same
	// message later echoes back through the live event handler (which
	// happens for multi-device sync of your own sent messages),
	// saveMessageOnce recognizes it and skips the duplicate instead of
	// inserting the same message twice.
	record := &models.WaMessage{
		UserID:      userID,
		DeviceID:    deviceID,
		ChatJID:     chatJID,
		MessageID:   string(resp.ID),
		FromMe:      true,
		Body:        body,
		MessageType: models.WaMessageTypeText,
		SentAt:      now,
	}
	s.saveMessageOnce(deviceID, chatJID, record)

	return record, nil
}

// SendPoll sends a native WhatsApp poll (a question with 2+ selectable
// options) through one of the user's connected devices and records it as
// an outgoing message — the CRM-facing "survey" building block. See
// models.WaMessageTypePoll's docblock for why a poll, specifically, is
// the one interactive message type this app builds on: WhatsApp actively
// blocks/deprioritizes button and list template messages sent from
// unofficial (non-Business-API) connections like this one, but a poll is
// an ordinary consumer-app feature with no such restriction.
//
// selectableCount is how many options a voter may pick at once; pass 1
// for an ordinary single-choice poll (the common case — most CSAT/survey
// polls should be single-choice, since whatsmeow's DecryptPollVote gives
// back whichever full set the voter last chose regardless of this
// number, so this only affects what WhatsApp's own client UI lets the
// voter tap).
func (s *WaInboxService) SendPoll(ctx context.Context, userID string, deviceID string, chatJID string, question string, options []string, selectableCount int) (*models.WaMessage, error) {
	if err := s.devices.AssertOwnership(userID, deviceID); err != nil {
		return nil, err
	}

	if len(options) < 2 {
		return nil, fmt.Errorf("wa: a poll needs at least 2 options")
	}
	if selectableCount < 1 {
		selectableCount = 1
	}
	if selectableCount > len(options) {
		selectableCount = len(options)
	}

	client, err := s.devices.EnsureConnectedClient(deviceID)
	if err != nil {
		return nil, err
	}

	jid, err := types.ParseJID(chatJID)
	if err != nil {
		return nil, fmt.Errorf("wa: invalid chat id: %w", err)
	}

	// Anti-ban backstop — see the identical guard in SendMessage above
	// for the full reasoning.
	release, err := s.devices.AcquireSendSlot(ctx, deviceID)
	if err != nil {
		return nil, fmt.Errorf("wa: timed out waiting to send: %w", err)
	}
	defer release()

	pollMsg := client.BuildPollCreation(question, options, selectableCount)
	resp, err := client.SendMessage(ctx, jid, pollMsg)
	if err != nil {
		return nil, fmt.Errorf("wa: failed to send poll: %w", err)
	}

	optionsJSON, err := json.Marshal(options)
	if err != nil {
		return nil, fmt.Errorf("wa: failed to encode poll options: %w", err)
	}

	now := time.Now()
	name := s.resolveChatName(ctx, deviceID, client, jid, "")
	// 📊 prefix mirrors the media types' emoji-prefixed chat preview
	// convention (see saveIncomingMedia), so a poll stands out in the
	// chat list the same way a photo/document already does instead of
	// showing the bare question text with no hint it's a poll.
	s.upsertChat(userID, deviceID, chatJID, name, "📊 "+question, now, false)

	record := &models.WaMessage{
		UserID:              userID,
		DeviceID:            deviceID,
		ChatJID:             chatJID,
		MessageID:           string(resp.ID),
		FromMe:              true,
		Body:                question,
		MessageType:         models.WaMessageTypePoll,
		PollOptions:         string(optionsJSON),
		PollSelectableCount: selectableCount,
		SentAt:              now,
	}
	s.saveMessageOnce(deviceID, chatJID, record)

	return record, nil
}

// handlePollVote decrypts an incoming poll vote/update and records the
// voter's current full selection in wa_poll_votes. Best-effort at every
// step: a vote is a nice-to-have signal, not something whose failure
// should ever propagate up into whatsmeow's own event loop.
//
// Two things make this different from a normal incoming message:
//  1. The vote only carries a *hash* of each chosen option (never the
//     original text) — whatsmeow.HashPollOptions recomputes the same
//     hashes from the poll's own stored option list so they can be
//     matched back to the human-readable text.
//  2. Votes fully REPLACE a voter's previous selection rather than
//     adding to it (that's how WhatsApp's own poll protocol works, not
//     a choice made here) — see WaPollVote's docblock for how the
//     upsert below mirrors that.
func (s *WaInboxService) handlePollVote(deviceID string, evt *events.Message) {
	pollUpdate := evt.Message.GetPollUpdateMessage()
	pollMessageID := pollUpdate.GetPollCreationMessageKey().GetID()
	if pollMessageID == "" {
		log.Printf("wa-inbox: handlePollVote: vote event missing its poll's original message id (device=%s) — skipped", deviceID)
		return
	}

	// The vote can only be resolved against a poll THIS app sent (we need
	// its original option list to recompute hashes against) — a poll
	// history-synced from before this feature existed, or one somehow
	// created outside this app, has nowhere to attach the vote to.
	var poll models.WaMessage
	err := s.db.
		Where(models.WaMessage{DeviceID: deviceID, MessageID: pollMessageID, MessageType: models.WaMessageTypePoll}).
		First(&poll).Error
	if err != nil {
		log.Printf("wa-inbox: handlePollVote: original poll message %s not found for device %s — vote skipped", pollMessageID, deviceID)
		return
	}

	client, ok := s.devices.GetClient(deviceID)
	if !ok || client == nil {
		return
	}

	vote, err := client.DecryptPollVote(context.Background(), evt)
	if err != nil {
		log.Printf("wa-inbox: handlePollVote: failed to decrypt vote for poll %s (device=%s): %v", pollMessageID, deviceID, err)
		return
	}

	var options []string
	if err := json.Unmarshal([]byte(poll.PollOptions), &options); err != nil {
		log.Printf("wa-inbox: handlePollVote: failed to parse stored options for poll %s: %v", pollMessageID, err)
		return
	}

	hashes := whatsmeow.HashPollOptions(options)
	selected := make([]string, 0, len(vote.GetSelectedOptions()))
	for _, chosenHash := range vote.GetSelectedOptions() {
		for i, optionHash := range hashes {
			if bytes.Equal(chosenHash, optionHash) {
				selected = append(selected, options[i])
				break
			}
		}
	}

	selectedJSON, err := json.Marshal(selected)
	if err != nil {
		log.Printf("wa-inbox: handlePollVote: failed to encode selected options for poll %s: %v", pollMessageID, err)
		return
	}

	voterJID := evt.Info.Sender.String()

	var voteRow models.WaPollVote
	s.db.
		Where(models.WaPollVote{DeviceID: deviceID, PollMessageID: pollMessageID, VoterJID: voterJID}).
		Assign(models.WaPollVote{
			ChatJID:         evt.Info.Chat.String(),
			SelectedOptions: string(selectedJSON),
			VotedAt:         evt.Info.Timestamp,
		}).
		FirstOrCreate(&voteRow)

	log.Printf("wa-inbox: handlePollVote: recorded vote on poll %s by %s: %v", pollMessageID, voterJID, selected)

	s.notifyPollVoteWebhook(deviceID, pollMessageID, evt.Info.Chat.String(), voterJID, selected)
}

// notifyPollVoteWebhook tells Laravel a poll vote just arrived — same
// fire-and-forget, best-effort shape as notifyIncomingMessageWebhook/
// notifyMessageStatusWebhook above (a slow/unreachable Laravel must never
// delay whatsmeow's own event processing, and there's no retry queue on
// this side). Generic on purpose: this app has exactly one consumer of
// poll votes today (Laravel's CSAT survey feature, matching on
// poll_message_id — see App\Http\Controllers\Api\
// WaPollVoteWebhookController), but nothing here is CSAT-specific, so any
// future poll-based feature can reuse this same event without another
// round trip through whatsmeow being needed.
func (s *WaInboxService) notifyPollVoteWebhook(deviceID, pollMessageID, chatJID, voterJID string, selectedOptions []string) {
	if s.laravelBaseURL == "" {
		return
	}

	payload, err := json.Marshal(map[string]any{
		"device_id":        deviceID,
		"poll_message_id":  pollMessageID,
		"chat_jid":         chatJID,
		"voter_jid":        voterJID,
		"selected_options": selectedOptions,
	})
	if err != nil {
		log.Printf("wa-inbox: failed to marshal poll-vote webhook payload: %v", err)
		return
	}

	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("wa-inbox: panic recovered in notifyPollVoteWebhook goroutine (device=%s pollMessageID=%s): %v", deviceID, pollMessageID, r)
			}
		}()

		req, err := http.NewRequest(http.MethodPost, s.laravelBaseURL+"/api/webhooks/wa/poll-vote", bytes.NewReader(payload))
		if err != nil {
			log.Printf("wa-inbox: failed to build poll-vote webhook request: %v", err)
			return
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-API-KEY", s.webhookAPIKey)

		resp, err := s.webhookClient.Do(req)
		if err != nil {
			log.Printf("wa-inbox: poll-vote webhook to Laravel failed: %v", err)
			return
		}
		defer resp.Body.Close()

		if resp.StatusCode >= 300 {
			bodyBytes, _ := io.ReadAll(resp.Body)
			log.Printf("wa-inbox: poll-vote webhook to Laravel returned status %d: %s", resp.StatusCode, string(bodyBytes))
		}
	}()
}

// PollResults returns a poll's current tally: the question/options it
// was sent with, plus every voter's current selection — the raw material
// for a CRM-side results view (e.g. "8/12 responded, 5x Puas / 3x Tidak
// Puas"). Counting/grouping is left to the caller (Laravel already has a
// precedent for doing aggregate reporting in SQL rather than in-process,
// see App\Services\Chat\ChatReportingService) since this is a small,
// per-poll result set, not a high-volume aggregate query.
func (s *WaInboxService) PollResults(userID string, deviceID string, pollMessageID string) (*models.WaMessage, []models.WaPollVote, error) {
	if err := s.devices.AssertOwnership(userID, deviceID); err != nil {
		return nil, nil, err
	}

	var poll models.WaMessage
	err := s.db.
		Where(models.WaMessage{DeviceID: deviceID, MessageID: pollMessageID, MessageType: models.WaMessageTypePoll}).
		First(&poll).Error
	if err != nil {
		return nil, nil, fmt.Errorf("wa: poll message not found: %w", err)
	}

	var votes []models.WaPollVote
	err = s.db.
		Where(models.WaPollVote{DeviceID: deviceID, PollMessageID: pollMessageID}).
		Order("voted_at ASC").
		Find(&votes).Error
	if err != nil {
		return nil, nil, err
	}

	return &poll, votes, nil
}

// upsertChat creates or refreshes a chat's summary row. incrementUnread
// should be true only for genuinely new incoming messages, never for
// messages the user just sent themselves.
func (s *WaInboxService) upsertChat(userID string, deviceID string, chatJID, name, lastMessage string, at time.Time, incrementUnread bool) {
	// Zero-value if no row exists yet, which is exactly the unread count
	// a brand new chat should start from.
	var existing models.WaChat
	s.db.Where(models.WaChat{DeviceID: deviceID, ChatJID: chatJID}).First(&existing)

	updates := models.WaChat{
		UserID:        userID,
		DeviceID:      deviceID,
		ChatJID:       chatJID,
		LastMessage:   lastMessage,
		LastMessageAt: at,
		UnreadCount:   existing.UnreadCount,
	}
	if name != "" {
		updates.Name = name
	}
	if incrementUnread {
		updates.UnreadCount++
	}

	var chat models.WaChat
	s.db.
		Where(models.WaChat{DeviceID: deviceID, ChatJID: chatJID}).
		Assign(updates).
		FirstOrCreate(&chat)
}

// resolveChatName picks the right label for a chat, and critically,
// treats groups completely differently from individual chats:
//   - Groups are named after the *group's* subject (fetched/cached via
//     groupName), never the push name of whoever happens to have sent the
//     message we're currently processing — otherwise the chat's displayed
//     name would flip to a different group member every time someone new
//     posts, which is the bug that was actually being seen.
//   - Individual chats use the sender's push name when we have one
//     (senderPushName), falling back to a resolved phone number
//     (displayNameFallback) when we don't.
func (s *WaInboxService) resolveChatName(ctx context.Context, deviceID string, client *whatsmeow.Client, chat types.JID, senderPushName string) string {
	if chat.Server == types.GroupServer {
		return s.groupName(ctx, deviceID, client, chat)
	}

	if chat.Server == "newsletter" {
		return s.newsletterName(ctx, deviceID, client, chat)
	}

	if senderPushName != "" {
		return senderPushName
	}

	return displayNameFallback(ctx, client, chat)
}

// newsletterName returns a WhatsApp Channel's actual title (e.g. "PST
// Digital News"), same cached-first pattern as groupName — channel names
// practically never change once created, so there's no reason to re-fetch
// on every incoming message. The one wrinkle: chats created before this
// function existed already have a wrong cached name (the raw "+<jid
// digits>" displayNameFallback used to produce for newsletters, which is
// exactly what showed up as "+120363..." in the UI) — looksLikeRawJID
// detects that specific shape and forces a re-fetch instead of trusting
// the bad cached value forever.
func (s *WaInboxService) newsletterName(ctx context.Context, deviceID string, client *whatsmeow.Client, newsletterJID types.JID) string {
	var existing models.WaChat
	hasExisting := s.db.Where(models.WaChat{DeviceID: deviceID, ChatJID: newsletterJID.String()}).First(&existing).Error == nil
	if hasExisting && existing.Name != "" && !looksLikeRawJID(existing.Name, newsletterJID) {
		return existing.Name
	}

	if client == nil {
		return ""
	}

	info, err := client.GetNewsletterInfo(ctx, newsletterJID)
	if err != nil || info == nil {
		return ""
	}

	return info.ThreadMeta.Name.Text
}

// looksLikeRawJID reports whether name is exactly the "+<jid user part>"
// placeholder displayNameFallback produces when a proper name couldn't be
// resolved — used to tell a genuinely-resolved cached name apart from a
// stale fallback that should be retried.
func looksLikeRawJID(name string, jid types.JID) bool {
	return strings.TrimPrefix(name, "+") == jid.User
}

// groupName returns a group's subject, preferring whatever we already
// have cached (group metadata rarely changes and fetching it is a network
// round trip) and only calling out to WhatsApp when nothing is cached yet.
func (s *WaInboxService) groupName(ctx context.Context, deviceID string, client *whatsmeow.Client, groupJID types.JID) string {
	var existing models.WaChat
	if err := s.db.Where(models.WaChat{DeviceID: deviceID, ChatJID: groupJID.String()}).First(&existing).Error; err == nil && existing.Name != "" {
		return existing.Name
	}

	if client == nil {
		return ""
	}

	info, err := client.GetGroupInfo(ctx, groupJID)
	if err != nil || info == nil {
		return ""
	}

	return info.GroupName.Name
}

// displayNameFallback produces a human-readable label for an individual
// (non-group) chat when WhatsApp hasn't given us a proper name (no push
// name, no saved contact). For normal chats, the JID's user part already
// *is* the phone number. For "@lid" chats — WhatsApp's privacy-preserving
// linked IDs — the JID's user part is an opaque internal ID, not a phone
// number, and has to be resolved through WhatsApp's own LID<->phone
// mapping instead; otherwise the UI ends up showing meaningless digits
// like "227414357594160" instead of the actual contact.
func displayNameFallback(ctx context.Context, client *whatsmeow.Client, jid types.JID) string {
	if jid.Server == types.GroupServer {
		return "" // groups are named via resolveChatName -> groupName instead
	}

	if jid.Server != "lid" {
		if jid.User == "" {
			return ""
		}
		return "+" + jid.User
	}

	if client == nil || client.Store == nil || client.Store.LIDs == nil {
		return ""
	}

	pn, err := client.Store.LIDs.GetPNForLID(ctx, jid)
	if err != nil || pn.User == "" {
		return "" // couldn't resolve — better left blank than showing noise
	}

	return "+" + pn.User
}

// extractText pulls plain text out of a WhatsApp message, whether it's a
// plain conversation message or an extended (quoted/formatted) one.
func extractText(msg *waE2E.Message) string {
	if msg == nil {
		return ""
	}
	if text := msg.GetConversation(); text != "" {
		return text
	}
	if ext := msg.GetExtendedTextMessage(); ext != nil {
		return ext.GetText()
	}
	return ""
}
