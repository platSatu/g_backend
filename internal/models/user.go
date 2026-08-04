package models

import "time"

// User maps to the shared `users` table used by both this backend and the
// Laravel application. Laravel generates a UUID (string) primary key for
// every user (see App\Models\User::booted()), not an auto-increment int —
// ID must stay a string to match. Only the columns needed for
// authentication are mapped here; extend as other parts of the system
// need more fields.
type User struct {
	ID        string    `gorm:"primaryKey;type:char(36)" json:"id"`
	Name      string    `json:"name"`
	Email     string    `gorm:"uniqueIndex" json:"email"`
	Password  string    `json:"-"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// TableName pins the GORM table name to "users" (Laravel's default),
// regardless of Go naming conventions.
func (User) TableName() string {
	return "users"
}
