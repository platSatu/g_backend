package service

import (
	"gorm.io/gorm"
)

// CompanyContext mirrors Laravel's App\Services\Company\CompanyContext —
// resolved from the shared MySQL tables the Laravel app owns (companies,
// company_to_users, branch_offices), so WhatsApp device visibility can
// follow the exact same owner/branch rules already enforced in Laravel's
// UI, without this backend keeping its own separate copy of company
// membership data.
type CompanyContext struct {
	IsOwner bool

	// Only meaningful when Resolved is true.
	CompanyID string

	// Empty string means "member with no branch office assigned yet" —
	// distinct from "unrestricted": an owner sees every branch
	// regardless of BranchOfficeID, but a non-owner member with an empty
	// BranchOfficeID has been placed in no branch at all and should see
	// no branch-scoped devices (see the ListDevices/assertOwnership
	// switch in wa-connectdevice-service.go).
	BranchOfficeID string

	// False if userID isn't part of any company at all (a standalone
	// account with no Company/CompanyToUser row — most existing
	// accounts, before this feature existed). Callers fall back to the
	// original plain per-user_id behavior in that case, so nothing that
	// predates company/branch support changes behavior.
	Resolved bool
}

// resolveCompanyContext looks up how userID relates to a company, mirroring
// App\Services\Company\CompanyContextResolver::resolve() in Laravel:
//
//   - Owner: a row in `companies` where user_id = userID.
//   - Member: an ACTIVE row in `company_to_users` for userID.
//
// Owner takes priority over membership, same tie-break Laravel uses (a
// user who both owns a company and happens to be listed as a member
// somewhere else is treated as the owner).
func resolveCompanyContext(db *gorm.DB, userID string) CompanyContext {
	var owned struct {
		ID string `gorm:"column:id"`
	}

	err := db.Table("companies").
		Select("id").
		Where("user_id = ?", userID).
		Limit(1).
		Take(&owned).Error

	if err == nil && owned.ID != "" {
		return CompanyContext{IsOwner: true, CompanyID: owned.ID, Resolved: true}
	}

	var membership struct {
		CompanyID      string  `gorm:"column:company_id"`
		BranchOfficeID *string `gorm:"column:branch_office_id"`
	}

	err = db.Table("company_to_users").
		Select("company_id, branch_office_id").
		Where("user_id = ? AND status = ?", userID, "active").
		Limit(1).
		Take(&membership).Error

	if err != nil || membership.CompanyID == "" {
		return CompanyContext{}
	}

	branchID := ""
	if membership.BranchOfficeID != nil {
		branchID = *membership.BranchOfficeID
	}

	return CompanyContext{
		IsOwner:        false,
		CompanyID:      membership.CompanyID,
		BranchOfficeID: branchID,
		Resolved:       true,
	}
}
