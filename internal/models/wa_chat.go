package models

import (
	"time"

	"gorm.io/gorm"

	"g_backend/internal/util"
)

// WaChat is a materialized summary of one conversation (1:1 or group) on
// one connected WhatsApp device. Scoped by DeviceID (not just UserID),
// since a user can have several devices, each with its own independent
// set of WhatsApp chats. Kept up to date whenever a message comes in or
// goes out, so listing chats never has to scan the full message history.
type WaChat struct {
	// ID is a random UUID, not an auto-increment integer — same reasoning
	// as WaDevice.ID (unguessable, consistent across every table in this
	// app). Nothing orders/paginates on WaChat.ID (chats are always
	// listed by LastMessageAt), so unlike WaMessage this table needs no
	// separate Seq column.
	ID       string `gorm:"primaryKey;type:char(36)" json:"id"`
	UserID   string `gorm:"column:user_id;type:char(36);not null;index" json:"user_id"`
	DeviceID string `gorm:"column:device_id;type:char(36);not null;uniqueIndex:idx_wa_chats_device_jid" json:"device_id"`

	// Explicit `column:` tags below on purpose: GORM's default naming
	// strategy recognizes "ID" as a unit (UserID -> user_id) but not
	// "JID" (ChatJID would otherwise become something like chat_j_id),
	// which caused a very confusing "unknown column" bug. Spelling out
	// the column name removes any guesswork.
	ChatJID       string    `gorm:"column:chat_jid;size:64;not null;uniqueIndex:idx_wa_chats_device_jid" json:"chat_jid"`
	Name          string    `gorm:"column:name;size:191" json:"name"`

	// Phone is the contact's real phone number ("+62..."), resolved once
	// and cached here — see WaInboxService.ensurePhone. Ordinary chats
	// already have the number sitting right in the JID, but "@lid" chats
	// (WhatsApp's privacy-preserving linked IDs) don't: their JID's user
	// part is an opaque internal ID, not a phone number, and has to be
	// resolved through WhatsApp's own LID<->phone mapping instead. Empty
	// for groups/channels, which have no phone number at all.
	Phone         string    `gorm:"column:phone;size:32" json:"phone,omitempty"`

	AvatarURL     string    `gorm:"column:avatar_url;size:512" json:"avatar_url"`

	// AvatarCheckedAt -- when ensureAvatar() last actually asked WhatsApp
	// for this contact's profile picture (whether or not one came back).
	// WhatsApp's profile-picture URLs are NOT permanent despite the old
	// assumption this cache was built on -- they expire, so a URL fetched
	// once and cached forever eventually 403s in the browser and the
	// avatar just silently stops rendering (exactly the "used to show,
	// now nothing shows, for every contact at once" symptom reported 22
	// September 2026, since most were originally cached around the same
	// early period and expired together). ensureAvatar() now re-fetches
	// once this gets stale instead of trusting avatar_url forever. Not
	// exposed in JSON (json:"-") -- purely an internal freshness marker.
	AvatarCheckedAt *time.Time `gorm:"column:avatar_checked_at" json:"-"`
	LastMessage   string    `gorm:"column:last_message;size:255" json:"last_message"`
	LastMessageAt time.Time `gorm:"column:last_message_at" json:"last_message_at"`

	// PresenceState/LastSeenAt persist WaConnectDeviceService's presence
	// tracking (online/typing/offline + last seen), which used to live
	// ONLY in an in-memory map — meaning it silently reset to nothing
	// every time g_backend restarted, even though nothing was actually
	// wrong with the presence subscription itself. Not exposed in the
	// /chats JSON response (json:"-") — presence is still fetched
	// through its own dedicated endpoint (WaInboxService.Presence), this
	// is purely the persisted backing store for that, not a new field
	// the frontend reads directly off a chat.
	PresenceState string     `gorm:"column:presence_state;size:16" json:"-"`
	LastSeenAt    *time.Time `gorm:"column:last_seen_at" json:"-"`
	UnreadCount   int       `gorm:"column:unread_count;not null;default:0" json:"unread_count"`

	// ParticipantCount is a cached group member count — 0 for non-group
	// chats, or a group whose size hasn't been fetched yet. Refreshed
	// opportunistically by WaInboxService.groupParticipantCount whenever
	// a group message receipt needs it and the cached value is still 0,
	// rather than on every single receipt (GetGroupInfo is a network
	// round trip to WhatsApp). Powers the "x/y dibaca" denominator
	// Laravel's Pesan Terjadwal history page shows for group recipients
	// — see WaMessageReceipt and UpdateMessageStatus's webhook payload.
	// Not exposed in the /chats JSON response (json:"-"); the frontend
	// has no use for it there, only Laravel's webhook consumer does.
	ParticipantCount int `gorm:"column:participant_count;not null;default:0" json:"-"`

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

func (WaChat) TableName() string {
	return "wa_chats"
}

// BeforeCreate assigns a random UUID before insert if one wasn't already
// set — same pattern as WaDevice.BeforeCreate.
func (c *WaChat) BeforeCreate(tx *gorm.DB) error {
	if c.ID == "" {
		c.ID = util.NewUUID()
	}
	return nil
}
