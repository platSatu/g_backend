package route

import (
	"github.com/gin-gonic/gin"

	"g_backend/internal/controllers"
	"g_backend/internal/middleware"
	"g_backend/internal/service"
)

// RegisterWaMediaRoutes wires up media send/download endpoints.
// Sending is nested under the same /wa/devices/:id/chats/:jid prefix as
// the plain-text inbox routes (a media message is still "sent to a
// chat"). Downloading is registered directly under /wa/devices/:id
// instead, since a stored media file isn't scoped to one chat once it
// exists — only to the device (and, transitively, the user) that owns
// it, which is all GetMediaFile actually checks.
func RegisterWaMediaRoutes(rg *gin.RouterGroup, mediaController *controllers.WaMediaController, authService *service.AuthService) {
	chats := rg.Group("/wa/devices/:id/chats")
	chats.Use(middleware.RequireAuth(authService))
	{
		chats.POST("/:jid/media", mediaController.SendMedia)
		chats.GET("/:jid/media-list", mediaController.ListMedia)
	}

	devices := rg.Group("/wa/devices/:id")
	devices.Use(middleware.RequireAuth(authService))
	{
		devices.GET("/media/:messageId", mediaController.DownloadMedia)
	}
}
