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
	AvatarURL     string    `gorm:"column:avatar_url;size:512" json:"avatar_url"`
	LastMessage   string    `gorm:"column:last_message;size:255" json:"last_message"`
	LastMessageAt time.Time `gorm:"column:last_message_at" json:"last_message_at"`
	UnreadCount   int       `gorm:"column:unread_count;not null;default:0" json:"unread_count"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}

func (WaChat) TableName() string {
	return "wa_chats"
}
