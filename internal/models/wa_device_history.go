package models

import "time"

// WaDeviceHistory is a timestamped log entry for one device's connection
// lifecycle — connected, disconnected, logged out, reconnect attempts —
// so a user can see WHY their device dropped instead of just a "Terputus"
// badge with no explanation. Written by WaConnectDeviceService's event
// handler (see eventHandler) and EnsureConnectedClient's reconnect
// attempts; read by the Connect Device page's per-device history view.
type WaDeviceHistory struct {
	ID        uint      `gorm:"primaryKey" json:"id"`
	DeviceID  string    `gorm:"column:device_id;type:char(36);not null;index" json:"device_id"`
	Event     string    `gorm:"column:event;size:32;not null" json:"event"`
	Detail    string    `gorm:"column:detail;size:255" json:"detail,omitempty"`
	CreatedAt time.Time `json:"created_at"`
}

func (WaDeviceHistory) TableName() string {
	return "wa_device_histories"
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
