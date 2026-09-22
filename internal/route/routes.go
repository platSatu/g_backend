package route

import (
	"context"
	"log"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"

	"g_backend/internal/config"
	"g_backend/internal/controllers"
	"g_backend/internal/middleware"
	"g_backend/internal/service"
)

// SetupRoutes wires up dependencies (services -> controllers) and
// registers every route group under /api. This is the single entry point
// main.go calls to build the whole HTTP API.
//
// Returns the WhatsApp device service so main.go's graceful-shutdown
// handling can tell it to close every live WhatsApp socket cleanly on
// the way out (WaConnectDeviceService.DisconnectAll) — everything else
// SetupRoutes builds is only reachable through the router it configures
// in place, but shutdown needs this one reference directly.
func SetupRoutes(router *gin.Engine, db *gorm.DB, cfg *config.Config) *service.WaConnectDeviceService {
	authService := service.NewAuthService(db, cfg.SecretAPIKey)
	authController := controllers.NewAuthController(authService)

	waService, err := service.NewWaConnectDeviceService(db, cfg.WhatsmeowDBPath)
	if err != nil {
		log.Fatalf("route: failed to initialize WhatsApp device service: %v", err)
	}
	waController := controllers.NewWaConnectDeviceController(waService)

	// WaInboxService needs the live client WaConnectDeviceService manages,
	// and WaConnectDeviceService needs somewhere to hand off incoming
	// messages — SetMessageStore closes that loop after both exist.
	waInboxService := service.NewWaInboxService(db, waService, cfg.MediaStoragePath, cfg.LaravelBaseURL, cfg.SecretAPIKey)
	waService.SetMessageStore(waInboxService)
	waInboxController := controllers.NewWaInboxController(waInboxService)

	// Reconnect every device that was last known "connected" back into
	// memory — without this, every device silently goes "not connected"
	// for SendMessage/SendMedia after any restart of this process (a
	// deploy, a crash, a VPS reboot), even though WhatsApp's own session
	// is still perfectly valid and the Connect Device page keeps showing
	// "Terhubung" the whole time (see RestoreSessions' docblock for the
	// full story). Runs in the background so a slow/unreachable device
	// can't delay the server from accepting requests; wired in only now
	// (after SetMessageStore) so any message that arrives mid-reconnect
	// is actually persisted instead of silently dropped.
	go waService.RestoreSessions(context.Background())

	// Proactive health-check: periodically catches any session whose
	// socket silently dropped without whatsmeow's own auto-reconnect (or
	// this backend's own events.Disconnected/LoggedOut handlers) bringing
	// it back — so a device stuck in that gap gets reconnected on its own
	// instead of sitting "Terhubung" in the DB while sends against it
	// keep failing until someone notices. See StartConnectionWatchdog's
	// docblock for the full reasoning.
	go waService.StartConnectionWatchdog(context.Background())

	// WaMediaController is a separate controller from WaInboxController
	// but shares the same WaInboxService instance (media methods live in
	// wa-media-service.go, added onto *WaInboxService) — one place
	// owning chat/message state, two controllers splitting up the HTTP
	// surface for maintainability.
	waMediaController := controllers.NewWaMediaController(waInboxService)

	// Every /api route requires the shared server-to-server API key. Per
	// user route, RequireAuth additionally verifies which user is calling.
	api := router.Group("/api")
	api.Use(middleware.RequireAPIKey(cfg.SecretAPIKey))
	{
		RegisterAuthRoutes(api, authController, authService)
		RegisterWaConnectDeviceRoutes(api, waController, authService)
		RegisterWaInboxRoutes(api, waInboxController, authService)
		RegisterWaMediaRoutes(api, waMediaController, authService)
	}

	return waService
}
