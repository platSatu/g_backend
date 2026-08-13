package models

import (
	"time"

	"gorm.io/gorm"

	"g_backend/internal/util"
)

// WaPollVote is one voter's CURRENT selection on a poll message this app
// sent (see WaMessage, MessageType == WaMessageTypePoll). WhatsApp polls
// have "replace" semantics — a voter changing their mind resends their
// FULL new selection, never a diff — so this table mirrors that: one row
// per (device, poll message, voter), overwritten in place on every new
// vote rather than accumulating a history of every past selection. See
// WaInboxService.handlePollVote, which is the only writer of this table.
type WaPollVote struct {
	// ID is a random UUID, not an auto-increment integer — same reasoning
	// as WaDevice.ID. Nothing orders/paginates on WaPollVote.ID (results
	// are listed by VotedAt), so no separate Seq column is needed here.
	ID string `gorm:"primaryKey;type:char(36)" json:"id"`

	// DeviceID + PollMessageID + VoterJID together form the natural key
	// this table upserts on — see the uniqueIndex tags below and
	// handlePollVote's Where(...).Assign(...).FirstOrCreate(...) call,
	// the same upsert pattern WaInboxService.upsertChat already uses.
	DeviceID      string `gorm:"column:device_id;type:char(36);not null;uniqueIndex:idx_wa_poll_votes_unique" json:"device_id"`
	PollMessageID string `gorm:"column:poll_message_id;size:64;not null;uniqueIndex:idx_wa_poll_votes_unique" json:"poll_message_id"`
	VoterJID      string `gorm:"column:voter_jid;size:64;not null;uniqueIndex:idx_wa_poll_votes_unique" json:"voter_jid"`

	// ChatJID is redundant with the poll message's own ChatJID (kept here
	// too, not just joined through WaMessage) purely so poll results can
	// be listed with a single flat query — see WaInboxService.PollResults.
	ChatJID string `gorm:"column:chat_jid;size:64;not null;index" json:"chat_jid"`

	// SelectedOptions is a JSON array of the option TEXT (already
	// resolved from WhatsApp's SHA-256 option hashes back to the
	// original human-readable strings by handlePollVote) currently
	// chosen by this voter — never the raw hashes, since a company using
	// this for CRM reporting wants to read "Puas" / "Tidak Puas", not
	// opaque byte strings.
	SelectedOptions string    `gorm:"column:selected_options;type:text" json:"selected_options"`
	VotedAt         time.Time `gorm:"column:voted_at" json:"voted_at"`
	CreatedAt       time.Time `json:"created_at"`
	UpdatedAt       time.Time `json:"updated_at"`
}

func (WaPollVote) TableName() string {
	return "wa_poll_votes"
}

// BeforeCreate assigns a random UUID before insert if one wasn't already
// set — same pattern as WaDevice.BeforeCreate.
func (v *WaPollVote) BeforeCreate(tx *gorm.DB) error {
	if v.ID == "" {
		v.ID = util.NewUUID()
	}
	return nil
}
