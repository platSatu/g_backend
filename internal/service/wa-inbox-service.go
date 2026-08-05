package service

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
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
// returns (i.e. right when a chat is opened, afterID == 0 in
// ListMessages). Without a cap, chats carrying a large history-synced
// backlog would return (and the frontend would fully re-render) their
// entire history on every single poll — a major source of the UI feeling
// heavy/laggy. Later polls only ask for messages newer than the last one
// they've already seen (see ListMessages' afterID parameter), which is a
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

	// avatarCheckedMu/avatarChecked remembers which (deviceID, chatJID)
	// pairs we've already resolved an avatar for (found one, or
	// confirmed the contact has none) so ListMessages doesn't have to
	// hit MySQL on every poll just to find out there's nothing new to
	// fetch. Profile picture URLs are long-lived, so this cache is kept
	// for the life of the process — no need to invalidate it on
	// reconnect.
	avatarCheckedMu sync.Mutex
	avatarChecked   map[string]map[string]bool

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
		avatarChecked:    make(map[string]map[string]bool),
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

	body := extractText(evt.Message)
	if body == "" {
		log.Printf("wa-inbox: SaveIncomingMessage: no extractable text body (device=%s) — skipped, no auto-reply webhook fired", deviceID)
		return // skip anything that's neither text nor a downloadable media type (reactions, etc.)
	}

	chatJID := evt.Info.Chat.String()

	client, _ := s.devices.GetClient(deviceID)
	name := s.resolveChatName(context.Background(), deviceID, client, evt.Info.Chat, evt.Info.PushName)

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
		s.notifyIncomingMessageWebhook(deviceID, userID, chatJID, string(evt.Info.ID), body, evt.Info.Timestamp)
	} else {
		log.Printf("wa-inbox: SaveIncomingMessage: message is from own device (fromMe) — auto-reply webhook intentionally skipped to avoid reply loops (device=%s)", deviceID)
	}
}

// notifyIncomingMessageWebhook tells Laravel a text message just arrived,
// for its "Auto Reply (Kata Kunci)" feature to act on (matching `body`
// against configured keywords and replying). Fire-and-forget in its own
// goroutine: a slow or unreachable Laravel must never delay/break
// whatsmeow's own event processing, which is why this isn't called
// synchronously inline in SaveIncomingMessage. Best-effort — if it fails,
// the message is still saved normally above, just no auto-reply fires for
// it; there's no retry queue on this side.
func (s *WaInboxService) notifyIncomingMessageWebhook(deviceID, userID, chatJID, messageID, body string, sentAt time.Time) {
	if s.laravelBaseURL == "" {
		return
	}

	payload, err := json.Marshal(map[string]any{
		"device_id":  deviceID,
		"user_id":    userID,
		"chat_jid":   chatJID,
		"message_id": messageID,
		"body":       body,
		"sent_at":    sentAt.UTC().Format(time.RFC3339),
	})
	if err != nil {
		log.Printf("wa-inbox: failed to marshal incoming-message webhook payload: %v", err)
		return
	}

	log.Printf("wa-inbox: notifyIncomingMessageWebhook: POSTing to %s/api/webhooks/wa/incoming-message (device=%s chat=%s messageID=%s)", s.laravelBaseURL, deviceID, chatJID, messageID)

	go func() {
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
// database-assigned auto-increment ID back onto the caller's own struct.
// SendMessage relies on this: its response to the frontend carries this
// ID, which the frontend then uses as the polling cursor (?after_id=) —
// if the ID never made it back (as it didn't when this took msg by
// value), the newly sent message would get re-fetched and appended a
// second time on the very next poll.
func (s *WaInboxService) saveMessageOnce(deviceID string, chatJID string, msg *models.WaMessage) {
	if msg.MessageID != "" {
		var existing models.WaMessage
		err := s.db.
			Where(models.WaMessage{DeviceID: deviceID, ChatJID: chatJID, MessageID: msg.MessageID}).
			First(&existing).Error
		if err == nil {
			msg.ID = existing.ID // already have it — still hand back its real ID
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
	return chats, err
}

// ListMessages returns a chat's message history.
//
//   - On the initial load (afterID == 0) it returns only the most recent
//     messageHistoryLimit messages, oldest first, and triggers the
//     one-time side effects of opening a chat: marking it read,
//     subscribing to presence, and fetching the contact's avatar.
//   - On subsequent polls (afterID > 0) it returns only messages with a
//     higher ID than that — a small delta the frontend can append,
//     instead of re-fetching (and re-rendering) the whole thread every
//     few seconds.
func (s *WaInboxService) ListMessages(userID string, deviceID string, chatJID string, afterID uint) ([]models.WaMessage, error) {
	if err := s.devices.AssertOwnership(userID, deviceID); err != nil {
		return nil, err
	}

	var messages []models.WaMessage

	if afterID > 0 {
		err := s.db.
			Where("device_id = ? AND chat_jid = ? AND id > ?", deviceID, chatJID, afterID).
			Order("id ASC").
			Find(&messages).Error
		s.attachMediaURLs(deviceID, messages)
		return messages, err
	}

	err := s.db.
		Where("device_id = ? AND chat_jid = ?", deviceID, chatJID).
		Order("id DESC").
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

// ensureAvatar fetches and caches a contact's profile picture URL the
// first time their chat is opened. WhatsApp profile picture URLs are
// long-lived, so we don't bother refreshing them once cached, and we
// remember (in memory) that we've already checked so repeated polling
// doesn't keep re-querying MySQL for the same answer.
func (s *WaInboxService) ensureAvatar(deviceID string, chatJID string) {
	if s.avatarAlreadyChecked(deviceID, chatJID) {
		return
	}
	defer s.markAvatarChecked(deviceID, chatJID)

	var chat models.WaChat
	if err := s.db.Where(models.WaChat{DeviceID: deviceID, ChatJID: chatJID}).First(&chat).Error; err == nil && chat.AvatarURL != "" {
		return
	}

	client, ok := s.devices.GetClient(deviceID)
	if !ok || client == nil {
		return
	}

	jid, err := types.ParseJID(chatJID)
	if err != nil {
		return
	}

	info, err := client.GetProfilePictureInfo(context.Background(), jid, nil)
	if err != nil || info == nil || info.URL == "" {
		return // no picture set, or WhatsApp declined the request — not fatal
	}

	s.db.Model(&models.WaChat{}).
		Where(models.WaChat{DeviceID: deviceID, ChatJID: chatJID}).
		Update("avatar_url", info.URL)
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

	for _, id := range evt.MessageIDs {
		var msg models.WaMessage
		err := s.db.
			Where(models.WaMessage{DeviceID: deviceID, ChatJID: evt.Chat.String(), MessageID: string(id), FromMe: true}).
			First(&msg).Error
		if err != nil {
			continue // not one of ours, or the receipt raced the message insert
		}

		if messageStatusRank(status) <= messageStatusRank(msg.Status) {
			continue // never move status backwards (e.g. a delayed "delivered" landing after "read" already did)
		}

		s.db.Model(&models.WaMessage{}).Where("id = ?", msg.ID).Update("status", status)
	}
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

func (s *WaInboxService) avatarAlreadyChecked(deviceID, chatJID string) bool {
	s.avatarCheckedMu.Lock()
	defer s.avatarCheckedMu.Unlock()
	return s.avatarChecked[deviceID][chatJID]
}

func (s *WaInboxService) markAvatarChecked(deviceID, chatJID string) {
	s.avatarCheckedMu.Lock()
	defer s.avatarCheckedMu.Unlock()
	if s.avatarChecked[deviceID] == nil {
		s.avatarChecked[deviceID] = make(map[string]bool)
	}
	s.avatarChecked[deviceID][chatJID] = true
}

// SendMessage sends a text message through one of the user's connected
// devices and records it as an outgoing message.
func (s *WaInboxService) SendMessage(ctx context.Context, userID string, deviceID string, chatJID, body string) (*models.WaMessage, error) {
	if err := s.devices.AssertOwnership(userID, deviceID); err != nil {
		return nil, err
	}

	client, ok := s.devices.GetClient(deviceID)
	if !ok || client == nil || !client.IsConnected() {
		return nil, fmt.Errorf("wa: device is not connected")
	}

	jid, err := types.ParseJID(chatJID)
	if err != nil {
		return nil, fmt.Errorf("wa: invalid chat id: %w", err)
	}

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

	if senderPushName != "" {
		return senderPushName
	}

	return displayNameFallback(ctx, client, chat)
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
