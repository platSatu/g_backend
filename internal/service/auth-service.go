package service

import (
	"errors"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"golang.org/x/crypto/bcrypt"
	"gorm.io/gorm"

	"g_backend/internal/models"
)

var (
	ErrInvalidCredentials = errors.New("invalid email or password")
	ErrInvalidToken       = errors.New("invalid or expired token")
)

const tokenTTL = 24 * time.Hour

// Claims is the JWT payload issued on login and expected on every
// authenticated request, regardless of which frontend the caller is
// (Laravel/Sanctum or Next.js/Node both authenticate through this same
// token — see AuthService.Login).
type Claims struct {
	UserID string `json:"user_id"`
	Email  string `json:"email"`
	jwt.RegisteredClaims
}

// AuthService contains all authentication business logic: verifying
// credentials and issuing/validating JWTs.
type AuthService struct {
	db        *gorm.DB
	secretKey string
}

func NewAuthService(db *gorm.DB, secretKey string) *AuthService {
	return &AuthService{db: db, secretKey: secretKey}
}

// Login looks up the user by email, verifies the password, and returns a
// signed JWT the client should send back as "Authorization: Bearer <token>"
// on subsequent requests.
func (s *AuthService) Login(email, password string) (string, *models.User, error) {
	var user models.User
	if err := s.db.Where("email = ?", email).First(&user).Error; err != nil {
		return "", nil, ErrInvalidCredentials
	}

	if !verifyPassword(user.Password, password) {
		return "", nil, ErrInvalidCredentials
	}

	token, err := s.generateToken(user)
	if err != nil {
		return "", nil, err
	}

	return token, &user, nil
}

// ValidateToken parses and verifies a JWT, returning its claims when valid.
// Used by the auth middleware to protect routes.
func (s *AuthService) ValidateToken(tokenString string) (*Claims, error) {
	claims := &Claims{}

	token, err := jwt.ParseWithClaims(tokenString, claims, func(t *jwt.Token) (interface{}, error) {
		return []byte(s.secretKey), nil
	})
	if err != nil || !token.Valid {
		return nil, ErrInvalidToken
	}

	return claims, nil
}

func (s *AuthService) generateToken(user models.User) (string, error) {
	claims := Claims{
		UserID: user.ID,
		Email:  user.Email,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(tokenTTL)),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
		},
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return token.SignedString([]byte(s.secretKey))
}

// verifyPassword compares a plain password against a bcrypt hash. Laravel
// hashes passwords with a "$2y$" prefix; it's normalized to "$2a$" here so
// hashes created by the Laravel app verify correctly against Go's bcrypt.
func verifyPassword(hashedPassword, plainPassword string) bool {
	normalized := strings.Replace(hashedPassword, "$2y$", "$2a$", 1)
	return bcrypt.CompareHashAndPassword([]byte(normalized), []byte(plainPassword)) == nil
}
