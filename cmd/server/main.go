package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"go.mau.fi/whatsmeow/proto/waCompanionReg"
	"go.mau.fi/whatsmeow/store"

	"g_backend/helper"
	"g_backend/internal/config"
	"g_backend/internal/models"
	"g_backend/internal/route"
)

// shutdownTimeout bounds how long graceful shutdown waits — for
// in-flight HTTP requests to finish, then for every live WhatsApp socket
// to close — before main() gives up waiting and exits anyway. Kept
// under the systemd unit's TimeoutStopSec (see deploy notes) so this
// code's own timeout is always what ends shutdown, not systemd's harsher
// SIGKILL landing first and cutting things off mid-sequence.
const shutdownTimeout = 15 * time.Second

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
	if err := db.AutoMigrate(&models.WaDevice{}, &models.WaChat{}, &models.WaMessage{}, &models.WaDeviceHistory{}, &models.WaPollVote{}, &models.WaMessageReceipt{}); err != nil {
		log.Fatalf("failed to migrate WhatsApp tables: %v", err)
	}

	// Build the HTTP server.
	router := gin.Default()
	router.Use(helper.CORSMiddleware())

	// Register all routes (auth, wa-connectdevice, ...). waService is
	// handed back so the shutdown sequence below can close every live
	// WhatsApp socket cleanly — see SetupRoutes' docblock.
	waService := route.SetupRoutes(router, db, cfg)

	// http.Server (not router.Run, which just wraps ListenAndServe and
	// blocks forever) so this process can actually stop accepting new
	// requests on demand via Shutdown() below, instead of only ever
	// being torn down mid-request by the OS.
	srv := &http.Server{
		Addr:    cfg.AppPort,
		Handler: router,
	}

	// Serve in the background so main() is free to block on the shutdown
	// signal below instead of on ListenAndServe itself — that's what
	// makes a controlled shutdown possible at all.
	go func() {
		log.Printf("server listening on %s", cfg.AppPort)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server failed to start: %v", err)
		}
	}()

	// Block until systemd (or an operator) asks this process to stop.
	// SIGTERM is what `systemctl stop`/`restart` and a plain `kill` send
	// by default; SIGINT is Ctrl+C for a manual/local run. Without this,
	// the process has no way to react to either — the OS just tears it
	// down mid-request and mid-WhatsApp-socket on every deploy, restart,
	// or crash-recovery, which is exactly what every restart of this
	// backend did before this: in-flight HTTP responses cut off, and
	// every linked device's WhatsApp connection dropped uncleanly
	// instead of closing tidily and picking back up via RestoreSessions.
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT)
	<-quit
	log.Println("shutdown signal received, shutting down gracefully...")

	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	// Stop accepting new HTTP requests and wait (bounded by ctx above)
	// for in-flight ones to finish first — an in-progress SendMessage/
	// SendPoll call gets to complete its response instead of being cut
	// off mid-request.
	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("HTTP server shutdown did not complete cleanly: %v", err)
	}

	// Only now, with no more HTTP requests being served, close every
	// live WhatsApp socket cleanly (not a logout — see DisconnectAll's
	// docblock for why that distinction matters).
	waService.DisconnectAll()

	log.Println("graceful shutdown complete")
}
