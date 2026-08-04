package middleware

import (
	"crypto/subtle"
	"net/http"

	"github.com/gin-gonic/gin"
)

// RequireAPIKey protects the whole API behind a shared secret sent as the
// "X-API-KEY" header. This is a server-to-server trust layer: it proves the
// caller is our own Laravel backend (the only party that knows the key),
// separate from RequireAuth, which proves *which user* is making the call.
//
// Note: this reuses cfg.SecretAPIKey for both the API key and JWT signing,
// as set up in .env. For stronger isolation, consider splitting these into
// two separate secrets in the future.
func RequireAPIKey(apiKey string) gin.HandlerFunc {
	return func(c *gin.Context) {
		provided := c.GetHeader("X-API-KEY")

		if provided == "" || subtle.ConstantTimeCompare([]byte(provided), []byte(apiKey)) != 1 {
			c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "invalid or missing API key"})
			return
		}

		c.Next()
	}
}
