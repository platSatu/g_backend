package service

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"

	"g_backend/internal/models"
	"g_backend/internal/util"
)

// ErrMediaNotFound is returned by GetMediaFile when a message either
// doesn't exist for the given device, or exists but never had a media
// file stored for it (e.g. a text message, or a media message whose
// download failed at the time it arrived).
var ErrMediaNotFound = errors.New("wa: media not found")

// This file adds media (image/video/audio/document/sticker) support on
// top of WaInboxService — same struct/dependencies as
// wa-inbox-service.go (plain text), just kept in its own file so the two
// concerns (text vs. media) stay easy to read and maintain separately.
// WaMediaController (in its own file/route group too) is the HTTP layer
// that calls into these methods.

// incomingMedia bundles a downloadable sub-message together with the
// metadata this app persists, so detectMediaMessage's type switch only
// has to happen once per incoming message.
type incomingMedia struct {
	kind         string
	downloadable whatsmeow.DownloadableMessage
	mimeType     string
	fileName     string
	caption      string
}

// detectMediaMessage checks whether an incoming waE2E.Message is one of
// the five media kinds whatsmeow can download (image, video, audio,
// document, sticker) and returns nil if it's none of those — the caller
// then falls back to extractText's plain-text handling.
func detectMediaMessage(msg *waE2E.Message) *incomingMedia {
	if msg == nil {
		return nil
	}

	switch {
	case msg.GetImageMessage() != nil:
		m := msg.GetImageMessage()
		return &incomingMedia{kind: models.WaMessageTypeImage, downloadable: m, mimeType: m.GetMimetype(), caption: m.GetCaption()}
	case msg.GetVideoMessage() != nil:
		m := msg.GetVideoMessage()
		return &incomingMedia{kind: models.WaMessageTypeVideo, downloadable: m, mimeType: m.GetMimetype(), caption: m.GetCaption()}
	case msg.GetAudioMessage() != nil:
		m := msg.GetAudioMessage()
		return &incomingMedia{kind: models.WaMessageTypeAudio, downloadable: m, mimeType: m.GetMimetype()}
	case msg.GetDocumentMessage() != nil:
		m := msg.GetDocumentMessage()
		return &incomingMedia{kind: models.WaMessageTypeDocument, downloadable: m, mimeType: m.GetMimetype(), fileName: m.GetFileName(), caption: m.GetCaption()}
	case msg.GetStickerMessage() != nil:
		m := msg.GetStickerMessage()
		return &incomingMedia{kind: models.WaMessageTypeSticker, downloadable: m, mimeType: m.GetMimetype()}
	default:
		return nil
	}
}

// saveIncomingMedia downloads+decrypts an incoming media message's
// attachment (whatsmeow.Client.Download does both) and stores it, same
// upsertChat/saveMessageOnce bookkeeping as the plain-text path in
// SaveIncomingMessage. A download failure (expired link, network hiccup)
// still records the message row — just with an empty MediaPath, so the
// chat isn't silently missing an entry — rather than dropping it
// entirely.
func (s *WaInboxService) saveIncomingMedia(deviceID string, userID string, evt *events.Message, media *incomingMedia) {
	chatJID := evt.Info.Chat.String()

	client, ok := s.devices.GetClient(deviceID)
	if !ok || client == nil {
		return // no live session to download through
	}

	data, err := client.Download(context.Background(), media.downloadable)
	if err != nil {
		data = nil
	}

	var storedPath string
	if len(data) > 0 {
		storedPath, _ = s.saveMediaFile(deviceID, string(evt.Info.ID), media.fileName, data)
	}

	name := s.resolveChatName(context.Background(), deviceID, client, evt.Info.Chat, evt.Info.PushName)

	preview := media.caption
	if preview == "" {
		preview = mediaPreviewLabel(media.kind)
	}
	s.upsertChat(userID, deviceID, chatJID, name, preview, evt.Info.Timestamp, !evt.Info.IsFromMe)

	s.saveMessageOnce(deviceID, chatJID, &models.WaMessage{
		UserID:      userID,
		DeviceID:    deviceID,
		ChatJID:     chatJID,
		MessageID:   string(evt.Info.ID),
		SenderJID:   evt.Info.Sender.String(),
		FromMe:      evt.Info.IsFromMe,
		Body:        media.caption,
		MessageType: media.kind,
		MimeType:    media.mimeType,
		FileName:    media.fileName,
		MediaSize:   int64(len(data)),
		MediaPath:   storedPath,
		SentAt:      evt.Info.Timestamp,
	})
}

// SendMedia uploads a file to WhatsApp's media servers and sends it as
// an image/video/audio/document/sticker message on one chat, mirroring
// SendMessage's shape (ownership check, connected-client check, upsert
// the chat, persist a WaMessage row) with the extra upload/local-copy
// steps media needs.
//
// asSticker forces sticker semantics regardless of the uploaded file's
// detected MIME type — WhatsApp stickers are sent via StickerMessage
// rather than ImageMessage, which can't be inferred from MIME type alone
// (a sticker upload is still just image/webp).
func (s *WaInboxService) SendMedia(ctx context.Context, userID string, deviceID string, chatJID string, data []byte, mimeType string, fileName string, caption string, asSticker bool) (*models.WaMessage, error) {
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

	kind, waMediaType := classifyMime(mimeType)
	if asSticker {
		kind = models.WaMessageTypeSticker
		waMediaType = whatsmeow.MediaImage
	}

	uploaded, err := client.Upload(ctx, data, waMediaType)
	if err != nil {
		return nil, fmt.Errorf("wa: failed to upload media: %w", err)
	}

	protoMsg, err := buildOutgoingMediaMessage(kind, uploaded, mimeType, fileName, caption)
	if err != nil {
		return nil, err
	}

	// Anti-ban backstop — see the identical guard in
	// WaInboxService.SendMessage (wa-inbox-service.go) for the full
	// reasoning. Deliberately acquired only around the actual message
	// send, not the client.Upload call above: uploading bytes to
	// WhatsApp's media CDN doesn't count as an outbound chat message and
	// can be slow for large files, so it shouldn't hold up other sends
	// queued for this device.
	release, err := s.devices.AcquireSendSlot(ctx, deviceID)
	if err != nil {
		return nil, fmt.Errorf("wa: timed out waiting to send: %w", err)
	}
	defer release()

	resp, err := client.SendMessage(ctx, jid, protoMsg)
	if err != nil {
		return nil, fmt.Errorf("wa: failed to send media message: %w", err)
	}

	// Our own copy on disk, so the chat doesn't depend on WhatsApp's
	// short-lived CDN links (they expire; re-deriving them needs
	// MediaKey, which we'd have to store separately just to re-fetch
	// something we already have the bytes for right now).
	storedPath, _ := s.saveMediaFile(deviceID, string(resp.ID), fileName, data)

	now := time.Now()
	name := s.resolveChatName(ctx, deviceID, client, jid, "")
	preview := caption
	if preview == "" {
		preview = mediaPreviewLabel(kind)
	}
	s.upsertChat(userID, deviceID, chatJID, name, preview, now, false)

	record := &models.WaMessage{
		UserID:      userID,
		DeviceID:    deviceID,
		ChatJID:     chatJID,
		MessageID:   string(resp.ID),
		FromMe:      true,
		Body:        caption,
		MessageType: kind,
		MimeType:    mimeType,
		FileName:    fileName,
		MediaSize:   int64(len(data)),
		MediaPath:   storedPath,
		SentAt:      now,
	}
	s.saveMessageOnce(deviceID, chatJID, record)
	s.attachMediaURL(deviceID, record)

	return record, nil
}

// ListMedia returns a chat's media messages of one kind — image, video,
// or document, the three tabs the Inbox detail panel's MEDIA & FILES
// section offers — newest first, capped at 100. Only messages that
// actually have a stored file (MediaPath != "") are returned; a media
// message whose download failed at the time it arrived has no file to
// show here.
func (s *WaInboxService) ListMedia(userID string, deviceID string, chatJID string, mediaType string) ([]models.WaMessage, error) {
	if err := s.devices.AssertOwnership(userID, deviceID); err != nil {
		return nil, err
	}

	switch mediaType {
	case models.WaMessageTypeImage, models.WaMessageTypeVideo, models.WaMessageTypeDocument:
		// valid, keep as-is
	default:
		mediaType = models.WaMessageTypeImage
	}

	var messages []models.WaMessage
	err := s.db.
		Where("device_id = ? AND chat_jid = ? AND message_type = ? AND media_path <> ''", deviceID, chatJID, mediaType).
		Order("seq DESC").
		Limit(100).
		Find(&messages).Error
	if err != nil {
		return nil, err
	}

	s.attachMediaURLs(deviceID, messages)
	return messages, nil
}

// GetMediaFile resolves a stored media file's absolute disk path for one
// message, after checking the caller actually owns the device it belongs
// to — the same ownership boundary every other per-device method in this
// package enforces. Controllers stream the file straight from the
// returned path; this layer only authorizes and locates it.
func (s *WaInboxService) GetMediaFile(userID string, deviceID string, messageID string) (path string, fileName string, mimeType string, err error) {
	if err := s.devices.AssertOwnership(userID, deviceID); err != nil {
		return "", "", "", err
	}

	var msg models.WaMessage
	if dbErr := s.db.Where("id = ? AND device_id = ?", messageID, deviceID).First(&msg).Error; dbErr != nil {
		return "", "", "", ErrMediaNotFound
	}

	if msg.MediaPath == "" {
		return "", "", "", ErrMediaNotFound
	}

	return filepath.Join(s.mediaStorageRoot, msg.MediaPath), msg.FileName, msg.MimeType, nil
}

// attachMediaURLs populates MediaURL (a computed, non-persisted field —
// see models.WaMessage) on every message in the slice that actually has
// stored media. Called right before a batch of messages is handed back
// to the controller/frontend.
func (s *WaInboxService) attachMediaURLs(deviceID string, messages []models.WaMessage) {
	for i := range messages {
		s.attachMediaURL(deviceID, &messages[i])
	}
}

func (s *WaInboxService) attachMediaURL(deviceID string, msg *models.WaMessage) {
	if msg.MediaPath == "" {
		return
	}
	msg.MediaURL = fmt.Sprintf("/api/wa/devices/%s/media/%s", deviceID, msg.ID)
}

// saveMediaFile writes decrypted media bytes to
// {mediaStorageRoot}/{deviceID}/{messageID}{ext} and returns the path
// relative to mediaStorageRoot (what's stored in WaMessage.MediaPath —
// relative so the whole media folder can be moved/mounted elsewhere
// without a data migration). One subfolder per device keeps files from
// different users' devices from ever colliding on the message ID alone.
func (s *WaInboxService) saveMediaFile(deviceID string, messageID string, fileName string, data []byte) (string, error) {
	ext := filepath.Ext(fileName)
	if ext == "" {
		ext = ".bin"
	}

	safeID := sanitizeForFilename(messageID)
	if safeID == "" {
		safeID = util.NewUUID()
	}

	dir := filepath.Join(s.mediaStorageRoot, deviceID)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("wa: failed to create media dir: %w", err)
	}

	relPath := filepath.Join(deviceID, safeID+ext)
	fullPath := filepath.Join(s.mediaStorageRoot, relPath)

	if err := os.WriteFile(fullPath, data, 0o644); err != nil {
		return "", fmt.Errorf("wa: failed to write media file: %w", err)
	}

	return relPath, nil
}

// sanitizeForFilename keeps a WhatsApp message ID filename-safe. IDs are
// normally already alphanumeric, but this is a small defensive pass
// since the value ends up as part of a real disk path.
func sanitizeForFilename(id string) string {
	var b strings.Builder
	for _, r := range id {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' {
			b.WriteRune(r)
		}
	}
	return b.String()
}

// mediaPreviewLabel is what shows up as a chat's "last message" preview
// (in the chat list) for a media message with no caption — same idea as
// WhatsApp's own "📷 Photo" / "🎥 Video" previews.
func mediaPreviewLabel(kind string) string {
	switch kind {
	case models.WaMessageTypeImage:
		return "📷 Foto"
	case models.WaMessageTypeVideo:
		return "🎥 Video"
	case models.WaMessageTypeAudio:
		return "🎵 Pesan suara"
	case models.WaMessageTypeDocument:
		return "📄 Dokumen"
	case models.WaMessageTypeSticker:
		return "Stiker"
	default:
		return "Media"
	}
}

// classifyMime maps an uploaded file's MIME type to our MessageType and
// the whatsmeow MediaType Upload needs (used to derive the right
// encryption keys — sending an image with the wrong MediaType produces a
// file WhatsApp's clients can't decrypt). Anything that isn't
// image/video/audio falls back to being sent as a document, which is the
// same thing WhatsApp's own clients do for unrecognized file types.
func classifyMime(mimeType string) (string, whatsmeow.MediaType) {
	switch {
	case strings.HasPrefix(mimeType, "image/"):
		return models.WaMessageTypeImage, whatsmeow.MediaImage
	case strings.HasPrefix(mimeType, "video/"):
		return models.WaMessageTypeVideo, whatsmeow.MediaVideo
	case strings.HasPrefix(mimeType, "audio/"):
		return models.WaMessageTypeAudio, whatsmeow.MediaAudio
	default:
		return models.WaMessageTypeDocument, whatsmeow.MediaDocument
	}
}

// buildOutgoingMediaMessage copies an UploadResponse's fields into the
// right waE2E sub-message type for `kind`. Field names/shapes here are
// taken directly from whatsmeow's own Upload doc example (URL,
// DirectPath, MediaKey, FileEncSHA256, FileSHA256, FileLength are
// identical across all five message types).
func buildOutgoingMediaMessage(kind string, uploaded whatsmeow.UploadResponse, mimeType string, fileName string, caption string) (*waE2E.Message, error) {
	switch kind {
	case models.WaMessageTypeImage:
		return &waE2E.Message{ImageMessage: &waE2E.ImageMessage{
			URL:           proto.String(uploaded.URL),
			DirectPath:    proto.String(uploaded.DirectPath),
			MediaKey:      uploaded.MediaKey,
			FileEncSHA256: uploaded.FileEncSHA256,
			FileSHA256:    uploaded.FileSHA256,
			FileLength:    proto.Uint64(uploaded.FileLength),
			Mimetype:      proto.String(mimeType),
			Caption:       proto.String(caption),
		}}, nil

	case models.WaMessageTypeVideo:
		return &waE2E.Message{VideoMessage: &waE2E.VideoMessage{
			URL:           proto.String(uploaded.URL),
			DirectPath:    proto.String(uploaded.DirectPath),
			MediaKey:      uploaded.MediaKey,
			FileEncSHA256: uploaded.FileEncSHA256,
			FileSHA256:    uploaded.FileSHA256,
			FileLength:    proto.Uint64(uploaded.FileLength),
			Mimetype:      proto.String(mimeType),
			Caption:       proto.String(caption),
		}}, nil

	case models.WaMessageTypeAudio:
		// PTT (push-to-talk / voice note) is deliberately always false
		// here — this endpoint is for uploaded audio *files*, not
		// recorded-in-app voice notes, which is what PTT signals to
		// WhatsApp's clients (changes how the bubble renders).
		return &waE2E.Message{AudioMessage: &waE2E.AudioMessage{
			URL:           proto.String(uploaded.URL),
			DirectPath:    proto.String(uploaded.DirectPath),
			MediaKey:      uploaded.MediaKey,
			FileEncSHA256: uploaded.FileEncSHA256,
			FileSHA256:    uploaded.FileSHA256,
			FileLength:    proto.Uint64(uploaded.FileLength),
			Mimetype:      proto.String(mimeType),
			PTT:           proto.Bool(false),
		}}, nil

	case models.WaMessageTypeDocument:
		docFileName := fileName
		if docFileName == "" {
			docFileName = "file"
		}
		return &waE2E.Message{DocumentMessage: &waE2E.DocumentMessage{
			URL:           proto.String(uploaded.URL),
			DirectPath:    proto.String(uploaded.DirectPath),
			MediaKey:      uploaded.MediaKey,
			FileEncSHA256: uploaded.FileEncSHA256,
			FileSHA256:    uploaded.FileSHA256,
			FileLength:    proto.Uint64(uploaded.FileLength),
			Mimetype:      proto.String(mimeType),
			FileName:      proto.String(docFileName),
			Caption:       proto.String(caption),
		}}, nil

	case models.WaMessageTypeSticker:
		// StickerMessage has no Caption field — WhatsApp stickers can't
		// carry one, so `caption` (if the caller passed one) is simply
		// dropped for this kind.
		return &waE2E.Message{StickerMessage: &waE2E.StickerMessage{
			URL:           proto.String(uploaded.URL),
			DirectPath:    proto.String(uploaded.DirectPath),
			MediaKey:      uploaded.MediaKey,
			FileEncSHA256: uploaded.FileEncSHA256,
			FileSHA256:    uploaded.FileSHA256,
			FileLength:    proto.Uint64(uploaded.FileLength),
			Mimetype:      proto.String(mimeType),
		}}, nil

	default:
		return nil, fmt.Errorf("wa: unsupported media kind %q", kind)
	}
}
