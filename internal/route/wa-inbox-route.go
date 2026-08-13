package route

import (
	"github.com/gin-gonic/gin"

	"g_backend/internal/controllers"
	"g_backend/internal/middleware"
	"g_backend/internal/service"
)

// RegisterWaInboxRoutes wires up endpoints for reading and sending chat
// messages on one of the authenticated user's connected WhatsApp devices.
// Nested under /wa/devices/:id so every chat/message route is naturally
// scoped to a single device.
func RegisterWaInboxRoutes(rg *gin.RouterGroup, inboxController *controllers.WaInboxController, authService *service.AuthService) {
	inbox := rg.Group("/wa/devices/:id/chats")
	inbox.Use(middleware.RequireAuth(authService))
	{
		inbox.GET("", inboxController.ListChats)
		inbox.GET("/:jid/messages", inboxController.ListMessages)
		inbox.POST("/:jid/messages", inboxController.SendMessage)
		inbox.POST("/:jid/polls", inboxController.SendPoll)
		inbox.GET("/:jid/polls/:message_id/results", inboxController.PollResults)
		inbox.GET("/:jid/presence", inboxController.Presence)
	}
}
