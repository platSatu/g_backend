package models

import (
	"time"

	"gorm.io/gorm"

	"g_backend/internal/util"
)

// WaDevice tracks one WhatsApp device connection. A user can own several
// (per product decision: "1 user pasti punya lebih dari 1 device"), so
// UserID is a plain index here, not unique — the device's own ID (below)
// is what every other table and API route addresses it by. The actual
// WhatsApp session/keys live in whatsmeow's own SQLite store; this row is
// only the application-facing status record used by the API and UI.
//
// ID is a random UUID, not an auto-increment integer: this value shows
// up directly in URLs (e.g. /dashboard/chat/inbox/{device}), and a
// sequential ID would let one user simply increment the number to probe
// other users' devices. Ownership is still always re-checked server-side
// regardless, but an unguessable ID removes that whole class of probing
// as a first line of defense.
type WaDevice struct {
	ID     string `gorm:"primaryKey;type:char(36)" json:"id"`
	UserID string `gorm:"column:user_id;type:char(36);not null;index" json:"user_id"`

	// Both nullable and set once, at creation time (see
	// WaConnectDeviceService.AddDevice) from the creating user's
	// resolved CompanyContext — never re-derived later, so a device
	// stays associated with the branch it was added under even if the
	// creator's own membership later changes. NULL for devices added by
	// a standalone user with no Company/CompanyToUser row at all
	// (pre-existing accounts, or anyone outside the company/branch
	// feature) — ListDevices falls back to the original per-user_id
	// behavior for those, so nothing that predates this changes.
	//
	// CompanyID lets an OWNER see every device across every branch of
	// their company in one query, without joining through
	// company_to_users for every other member's devices.
	// BranchOfficeID is what actually scopes a non-owner member to only
	// their own branch's devices. Both use *string (not plain string)
	// so "no company" is distinguishable from "" as an empty column
	// value.
	CompanyID      *string `gorm:"column:company_id;type:char(36);index" json:"company_id"`
	BranchOfficeID *string `gorm:"column:branch_office_id;type:char(36);index" json:"branch_office_id"`

	// Explicit column:"jid" for the same reason as WaChat/WaMessage:
	// GORM's naming strategy doesn't treat "JID" as a recognized unit.
	JID         string     `gorm:"column:jid;size:64" json:"jid"`
	PhoneNumber string     `gorm:"column:phone_number;size:32" json:"phone_number"`
	Status      string     `gorm:"column:status;size:32;not null;default:disconnected" json:"status"`
	ConnectedAt *time.Time `json:"connected_at"`
	CreatedAt   time.Time  `json:"created_at"`
	UpdatedAt   time.Time  `json:"updated_at"`
}

func (WaDevice) TableName() string {
	return "wa_devices"
}

// BeforeCreate assigns a random UUID before insert if one wasn't already
// set, so callers never need to generate it themselves.
func (d *WaDevice) BeforeCreate(tx *gorm.DB) error {
	if d.ID == "" {
		d.ID = util.NewUUID()
	}
	return nil
}

// Status values for WaDevice.Status.
const (
	WaStatusDisconnected = "disconnected"
	WaStatusPendingQR    = "pending_qr"
	WaStatusConnected    = "connected"
)
