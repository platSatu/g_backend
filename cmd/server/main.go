package main

import (
	"log"

	"github.com/gin-gonic/gin"
	"go.mau.fi/whatsmeow/proto/waCompanionReg"
	"go.mau.fi/whatsmeow/store"

	"g_backend/helper"
	"g_backend/internal/config"
	"g_backend/internal/models"
	"g_backend/internal/route"
)

func main() {
	// Load configuration from .env / OS environment.
	cfg := config.LoadConfig()

	// Brands every linked device with "WhatsApp (Web)" (instead of
	// whatsmeow's default "whatsmeow" / a bare OS name) on WhatsApp's own
	// "Linked Devices" screen on the phone — store.DeviceProps is a
	// package-level global read by whatsmeow at pairing time (baked into
	// the registration payload), so this must run before any
	// whatsmeow.NewClient(...) call, which is why it's the very first
	// thing main() does. CLOUD_API is the platform-type value semantically
	// meant for an unattended API/bot session, as opposed to CHROME/EDGE/
	// etc. which would render with a literal browser icon on the phone.
	//
	// Named to read like the official WhatsApp Web session (previously
	// "Konexa API") rather than a third-party integration — consistent
	// with the anti-ban posture the rest of this app already takes (see
	// BroadcastThrottleService/BroadcastOptOutService on the Laravel
	// side): a name that doesn't advertise itself as unofficial tooling
	// is less likely to draw scrutiny than one that does.
	//
	// Only affects devices linked (or re-linked) AFTER this deploys —
	// whatsmeow doesn't retroactively rename an already-paired session,
	// since the name was already sent to WhatsApp once at pairing time.
	store.SetOSInfo("WhatsApp (Web)", [3]uint32{1, 0, 0})
	store.DeviceProps.PlatformType = waCompanionReg.DeviceProps_CLOUD_API.Enum()

	// Connect to the "teleios" database.
	db := config.ConnectDB(cfg)

	// Keep this backend's own tables in sync. The `users` table (and other
	// business tables) belong to the Laravel app and are intentionally
	// never auto-migrated here.
	if err := db.AutoMigrate(&models.WaDevice{}, &models.WaChat{}, &models.WaMessage{}, &models.WaDeviceHistory{}, &models.WaPollVote{}); err != nil {
		log.Fatalf("failed to migrate WhatsApp tables: %v", err)
	}

	// Build the HTTP server.
	router := gin.Default()
	router.Use(helper.CORSMiddleware())

	// Register all routes (auth, wa-connectdevice, ...).
	route.SetupRoutes(router, db, cfg)

	// Start listening.
	log.Printf("server listening on %s", cfg.AppPort)
	if err := router.Run(cfg.AppPort); err != nil {
		log.Fatalf("server failed to start: %v", err)
	}
}
