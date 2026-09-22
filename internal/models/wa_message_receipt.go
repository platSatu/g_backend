package models

import (
	"time"

	"gorm.io/gorm"

	"g_backend/internal/util"
)

// WaMessageReceipt is ONE participant's current delivery/read status for
// a message WE sent (WaMessage.FromMe == true) — one row per (message,
// participant), upserted as *events.Receipt events arrive from
// whatsmeow (see WaInboxService.UpdateMessageStatus).
//
// For an individual (non-group) chat there's naturally only ever one
// participant (the chat partner themselves, evt.Sender == evt.Chat), so
// this table also covers that case with exactly one row. WaMessage.Status
// is kept unchanged too (still the "has ANYONE progressed this far"
// ratchet, used by the single ✓✓ tick icon in the inbox UI) — but only
// THIS table can answer "how many of a GROUP's members actually read
// this", which a single status column can never express. WhatsApp sends
// a separate delivery/read receipt per group member (each with their own
// Sender), whereas the old code collapsed all of them onto one shared
// WaMessage.Status, which is why a group broadcast's "read" count could
// never show more than a binary yes/no regardless of how many members
// actually read it.
type WaMessageReceipt struct {
	ID string `gorm:"primaryKey;type:char(36)" json:"id"`

	WaMessageID string `gorm:"column:wa_message_id;type:char(36);not null;uniqueIndex:idx_wa_message_receipts_unique" json:"wa_message_id"`

	// ParticipantJID -- the group member who sent this receipt
	// (evt.Sender), or the chat partner themselves for a 1:1 chat
	// (Sender == Chat there).
	ParticipantJID string `gorm:"column:participant_jid;size:64;not null;uniqueIndex:idx_wa_message_receipts_unique" json:"participant_jid"`

	// Status -- one of WaMessageStatusDelivered/Read/Played. Never
	// 'sent' (that's the message's own starting state before ANY
	// receipt arrives — nothing to record per participant until then)
	// and never downgraded once written, same ratchet rule as
	// messageStatusRank applies to WaMessage.Status, just scoped to one
	// participant instead of the whole message.
	Status string `gorm:"column:status;size:16;not null" json:"status"`

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

func (WaMessageReceipt) TableName() string {
	return "wa_message_receipts"
}

// BeforeCreate assigns a random UUID before insert if one wasn't already
// set — same pattern as WaDevice.BeforeCreate.
func (r *WaMessageReceipt) BeforeCreate(tx *gorm.DB) error {
	if r.ID == "" {
		r.ID = util.NewUUID()
	}
	return nil
}
