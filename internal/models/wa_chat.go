package models

import "time"

// WaChat is a materialized summary of one conversation (1:1 or group) on
// one connected WhatsApp device. Scoped by DeviceID (not just UserID),
// since a user can have several devices, each with its own independent
// set of WhatsApp chats. Kept up to date whenever a message comes in or
// goes out, so listing chats never has to scan the full message history.
type WaChat struct {
	ID       uint   `gorm:"primaryKey" json:"id"`
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
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}

func (WaChat) TableName() string {
	return "wa_chats"
}
