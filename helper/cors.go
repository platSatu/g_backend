package helper

import (
	"net/http"
	"os"
	"strings"

	"github.com/gin-gonic/gin"
)

// CORSMiddleware controls which origins are allowed to call this backend.
// By default it allows the local Laravel (Sanctum) and Next.js/Node dev
// servers. In production, set CORS_ALLOWED_ORIGINS (comma separated) to
// the real frontend domains.
func CORSMiddleware() gin.HandlerFunc {
	allowedOrigins := allowedOriginsFromEnv()

	return func(c *gin.Context) {
		origin := c.Request.Header.Get("Origin")

		if isOriginAllowed(origin, allowedOrigins) {
			c.Writer.Header().Set("Access-Control-Allow-Origin", origin)
			c.Writer.Header().Set("Access-Control-Allow-Credentials", "true")
		}

		c.Writer.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS")
		c.Writer.Header().Set("Access-Control-Allow-Headers", "Origin, Content-Type, Accept, Authorization, X-Requested-With")

		if c.Request.Method == http.MethodOptions {
			c.AbortWithStatus(http.StatusNoContent)
			return
		}

		c.Next()
	}
}

// allowedOriginsFromEnv reads CORS_ALLOWED_ORIGINS if set, otherwise
// falls back to common local dev origins for Laravel and Next.js.
func allowedOriginsFromEnv() []string {
	if raw := os.Getenv("CORS_ALLOWED_ORIGINS"); raw != "" {
		origins := strings.Split(raw, ",")
		for i := range origins {
			origins[i] = strings.TrimSpace(origins[i])
		}
		return origins
	}

	return []string{
		"http://localhost:3000", // Next.js dev server
		"http://127.0.0.1:3000",
		"http://localhost:8000", // Laravel dev server
		"http://127.0.0.1:8000",
	}
}

func isOriginAllowed(origin string, allowed []string) bool {
	if origin == "" {
		return false
	}
	for _, o := range allowed {
		if o == origin {
			return true
		}
	}
	return false
}
