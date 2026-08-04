package main

import (
	"log"

	"github.com/gin-gonic/gin"

	"g_backend/helper"
	"g_backend/internal/config"
	"g_backend/internal/models"
	"g_backend/internal/route"
)

func main() {
	// Load configuration from .env / OS environment.
	cfg := config.LoadConfig()

	// Connect to the "teleios" database.
	db := config.ConnectDB(cfg)

	// Keep this backend's own tables in sync. The `users` table (and other
	// business tables) belong to the Laravel app and are intentionally
	// never auto-migrated here.
	if err := db.AutoMigrate(&models.WaDevice{}, &models.WaChat{}, &models.WaMessage{}); err != nil {
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
