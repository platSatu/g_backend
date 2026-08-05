package models

import "time"

// WaMessage is a single WhatsApp message belonging to one chat on one
// connected device. Text and media (image/video/audio/document/sticker)
// share this one table — MessageType tells the two apart, and the
// media-only columns are simply empty for plain text messages.
type WaMessage struct {
	ID       uint   `gorm:"primaryKey" json:"id"`
	UserID   string `gorm:"column:user_id;type:char(36);not null;index" json:"user_id"`
	DeviceID string `gorm:"column:device_id;type:char(36);not null;index:idx_wa_messages_device_chat" json:"device_id"`

	// Explicit `column:` tags: see the same note in wa_chat.go — GORM
	// doesn't recognize "JID" as a unit like it does "ID".
	ChatJID string `gorm:"column:chat_jid;size:64;not null;index:idx_wa_messages_device_chat" json:"chat_jid"`

	// WhatsApp's own message ID. Used to avoid storing duplicates when
	// the same message arrives twice (e.g. a history sync replays
	// something we already have live) — see WaInboxService.saveMessageOnce.
	MessageID string `gorm:"column:message_id;size:64;index" json:"message_id"`
	SenderJID string `gorm:"column:sender_jid;size:64" json:"sender_jid"`
	FromMe    bool   `gorm:"column:from_me;not null;default:false" json:"from_me"`

	// Status tracks WhatsApp's own delivery/read receipts for messages we
	// sent (FromMe == true) — see WaInboxService.UpdateMessageStatus,
	// which advances this as *events.Receipt events arrive from
	// whatsmeow. Meaningless for incoming messages (nobody reports
	// delivery/read state back to us for those), left at the default.
	// One of the WaMessageStatus* constants below.
	Status string `gorm:"column:status;size:16;not null;default:sent" json:"status"`

	// Body doubles as the media caption for MessageType != "text" — same
	// column, no separate caption field, since a message never has both.
	Body string `gorm:"column:body;type:text" json:"body"`

	// MessageType distinguishes plain text from the five media kinds
	// whatsmeow supports downloading (see wa-media-service.go). Defaults
	// to "text" so every pre-existing row (from before this column
	// existed) reads back as a normal text message.
	MessageType string `gorm:"column:message_type;size:16;not null;default:text" json:"message_type"`
	MimeType    string `gorm:"column:mime_type;size:128" json:"mime_type,omitempty"`
	FileName    string `gorm:"column:file_name;size:255" json:"file_name,omitempty"`
	MediaSize   int64  `gorm:"column:media_size" json:"media_size,omitempty"`

	// MediaPath is where the decrypted file lives on our own disk
	// (relative to the media storage root) — an internal detail, never
	// serialized to the frontend directly (json:"-"). MediaURL below is
	// what the frontend actually gets: an authenticated path back into
	// this API that streams MediaPath's contents, computed at response
	// time (gorm:"-" — not a real column) rather than stored, so it's
	// always correct even if the storage root ever moves.
	MediaPath string `gorm:"column:media_path;size:512" json:"-"`
	MediaURL  string `gorm:"-" json:"media_url,omitempty"`

	SentAt    time.Time `gorm:"column:sent_at" json:"sent_at"`
	CreatedAt time.Time `json:"created_at"`
}

func (WaMessage) TableName() string {
	return "wa_messages"
}

// Message type values for WaMessage.MessageType.
const (
	WaMessageTypeText     = "text"
	WaMessageTypeImage    = "image"
	WaMessageTypeVideo    = "video"
	WaMessageTypeAudio    = "audio"
	WaMessageTypeDocument = "document"
	WaMessageTypeSticker  = "sticker"
)

// Status values for WaMessage.Status — mirrors WhatsApp's own delivery
// receipt progression (sent -> delivered -> read/played). Ordered
// weakest to strongest; see messageStatusRank in wa-inbox-service.go,
// which uses this order to make sure a late/out-of-order receipt can
// never move a message's status backwards (e.g. a delayed "delivered"
// arriving after a "read" already landed must not downgrade it).
const (
	WaMessageStatusSent      = "sent"
	WaMessageStatusDelivered = "delivered"
	WaMessageStatusRead      = "read"
	WaMessageStatusPlayed    = "played"
)
