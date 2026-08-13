package models

import (
	"time"

	"gorm.io/gorm"

	"g_backend/internal/util"
)

// WaDeviceHistory is a timestamped log entry for one device's connection
// lifecycle — connected, disconnected, logged out, reconnect attempts —
// so a user can see WHY their device dropped instead of just a "Terputus"
// badge with no explanation. Written by WaConnectDeviceService's event
// handler (see eventHandler) and EnsureConnectedClient's reconnect
// attempts; read by the Connect Device page's per-device history view.
type WaDeviceHistory struct {
	// ID is a random UUID, not an auto-increment integer — same reasoning
	// as WaDevice.ID.
	ID string `gorm:"primaryKey;type:char(36)" json:"id"`

	// Seq is a plain auto-increment counter kept only so
	// GetDeviceHistory's "newest first" query has something reliable to
	// sort by — a random UUID has no natural order, and CreatedAt alone
	// can tie when several events land in the same second (e.g. a rapid
	// disconnect->reconnect pair). Not meant to be read by callers.
	Seq uint64 `gorm:"autoIncrement;not null;uniqueIndex" json:"-"`

	DeviceID  string    `gorm:"column:device_id;type:char(36);not null;index" json:"device_id"`
	Event     string    `gorm:"column:event;size:32;not null" json:"event"`
	Detail    string    `gorm:"column:detail;size:255" json:"detail,omitempty"`
	CreatedAt time.Time `json:"created_at"`
}

func (WaDeviceHistory) TableName() string {
	return "wa_device_histories"
}

// BeforeCreate assigns a random UUID before insert if one wasn't already
// set — same pattern as WaDevice.BeforeCreate.
func (h *WaDeviceHistory) BeforeCreate(tx *gorm.DB) error {
	if h.ID == "" {
		h.ID = util.NewUUID()
	}
	return nil
}

// Event values for WaDeviceHistory.Event.
const (
	WaDeviceEventConnected       = "connected"
	WaDeviceEventDisconnected    = "disconnected"
	WaDeviceEventLoggedOut       = "logged_out"
	WaDeviceEventPendingQR       = "pending_qr"
	WaDeviceEventReconnecting    = "reconnecting"
	WaDeviceEventReconnected     = "reconnected"
	WaDeviceEventReconnectFailed = "reconnect_failed"
	WaDeviceEventManualDisconnect = "manual_disconnect"
)
