package route

import (
	"github.com/gin-gonic/gin"

	"g_backend/internal/controllers"
	"g_backend/internal/middleware"
	"g_backend/internal/service"
)

// RegisterAuthRoutes wires up authentication endpoints. /login is public;
// /me demonstrates a protected route resolved via the JWT issued at login.
func RegisterAuthRoutes(rg *gin.RouterGroup, authController *controllers.AuthController, authService *service.AuthService) {
	auth := rg.Group("/auth")
	{
		auth.POST("/login", authController.Login)
		auth.GET("/me", middleware.RequireAuth(authService), authController.Me)
	}
}
