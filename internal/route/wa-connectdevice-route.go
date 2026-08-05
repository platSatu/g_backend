package route

import (
	"github.com/gin-gonic/gin"

	"g_backend/internal/controllers"
	"g_backend/internal/middleware"
	"g_backend/internal/service"
)

// RegisterWaConnectDeviceRoutes wires up endpoints for connecting and
// managing the authenticated user's WhatsApp devices (via whatsmeow). A
// user can own several devices, so every action below the collection
// route is scoped by :id and re-checked for ownership in the service
// layer.
func RegisterWaConnectDeviceRoutes(rg *gin.RouterGroup, waController *controllers.WaConnectDeviceController, authService *service.AuthService) {
	wa := rg.Group("/wa")
	wa.Use(middleware.RequireAuth(authService))
	{
		wa.GET("/devices", waController.ListDevices)
		wa.POST("/devices", waController.AddDevice)
		wa.GET("/devices/:id/status", waController.Status)
		wa.GET("/devices/:id/history", waController.History)
		wa.POST("/devices/:id/reconnect", waController.Reconnect)
		wa.POST("/devices/:id/disconnect", waController.Disconnect)
	}
}
