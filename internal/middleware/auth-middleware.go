package middleware

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"g_backend/internal/service"
)

// RequireAuth protects a route group behind a valid JWT, issued by
// AuthService.Login. Both frontends (Laravel/Sanctum and Next.js/Node)
// authenticate against this same middleware by sending
// "Authorization: Bearer <token>".
func RequireAuth(authService *service.AuthService) gin.HandlerFunc {
	return func(c *gin.Context) {
		header := c.GetHeader("Authorization")
		if header == "" || !strings.HasPrefix(header, "Bearer ") {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "missing or malformed authorization header"})
			return
		}

		tokenString := strings.TrimPrefix(header, "Bearer ")

		claims, err := authService.ValidateToken(tokenString)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid or expired token"})
			return
		}

		c.Set("user_id", claims.UserID)
		c.Set("user_email", claims.Email)
		c.Next()
	}
}
