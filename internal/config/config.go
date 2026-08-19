package config

import (
	"log"
	"os"
	"strconv"

	"github.com/joho/godotenv"
)

// Config holds every configuration value the application needs, loaded
// once at startup from the .env file / OS environment.
type Config struct {
	AppPort      string
	SecretAPIKey string

	// LaravelBaseURL is where the Laravel app answers webhook calls —
	// right now, only WaInboxService's "an incoming WhatsApp message just
	// arrived" notification (POST {LaravelBaseURL}/api/webhooks/wa/incoming-message),
	// which drives Laravel's "Auto Reply (Kata Kunci)" feature. Signed
	// with the same SECRET_API_KEY used the other direction (Laravel ->
	// Go), sent as X-API-KEY, so no new shared secret to manage.
	LaravelBaseURL string

	DBUser     string
	DBPassword string
	DBHost     string
	DBPort     string
	DBName     string

	// WhatsmeowDBPath is the SQLite file whatsmeow uses to store WhatsApp
	// device sessions/keys. Kept separate from the main MySQL database
	// because whatsmeow's official store only supports SQLite/Postgres.
	WhatsmeowDBPath string

	// MediaStoragePath is where decrypted media files (sent and
	// received) are kept on local disk, one subfolder per device — see
	// WaInboxService.saveMediaFile. Separate from WhatsmeowDBPath since
	// it holds plain files, not a SQLite database.
	MediaStoragePath string

	// DBMaxOpenConns/DBMaxIdleConns/DBConnMaxLifetimeMinutes size the
	// MySQL connection pool (see config.ConnectDB) — overridable via
	// .env rather than hardcoded, since the right number depends on how
	// many devices/companies this deployment actually carries, not on
	// this codebase. A device sending (broadcast recipient, AI Bot
	// reply, manual chat, etc.) does a handful of writes per message
	// (upsertChat + saveMessageOnce, see wa-inbox-service.go), so the
	// pool needs headroom for however many devices can realistically be
	// sending AT THE SAME INSTANT, not the total device count — e.g. 25
	// was sized for early single-tenant testing and is already tight
	// once a few dozen devices/companies are broadcasting concurrently
	// (see this Go backend's CLAUDE.md-equivalent audit notes on the
	// Laravel side for the fuller anti-ban/scaling picture).
	//
	// IMPORTANT: this pool is per PROCESS — if this backend is ever run
	// as more than one instance against the same "teleios" database (see
	// ConnectDB), the effective total is DBMaxOpenConns × instance
	// count, and that total (plus whatever Laravel's own connections to
	// the same database add) must stay under MySQL's own
	// `max_connections` (check with `SHOW VARIABLES LIKE
	// 'max_connections';`) — raising this value here without checking
	// that ceiling first just trades a slow queue for outright
	// "too many connections" errors under load.
	DBMaxOpenConns           int
	DBMaxIdleConns           int
	DBConnMaxLifetimeMinutes int
}

// LoadConfig reads the .env file (if present) and the OS environment,
// then returns a populated Config. Missing required values cause the
// application to fail fast instead of running in a broken state.
func LoadConfig() *Config {
	if err := godotenv.Load(); err != nil {
		log.Println("warning: .env file not found, relying on OS environment variables")
	}

	cfg := &Config{
		AppPort:      getEnv("APP_PORT", ":8080"),
		SecretAPIKey: getEnv("SECRET_API_KEY", ""),

		DBUser:     getEnv("DB_USER", "root"),
		DBPassword: getEnv("DB_PASSWORD", ""),
		DBHost:     getEnv("DB_HOST", "127.0.0.1"),
		DBPort:     getEnv("DB_PORT", "3306"),
		DBName:     getEnv("DB_NAME", ""),

		WhatsmeowDBPath: getEnv("WHATSMEOW_DB_PATH", "./storage/whatsmeow.db"),

		MediaStoragePath: getEnv("MEDIA_STORAGE_PATH", "./storage/media"),

		LaravelBaseURL: getEnv("LARAVEL_BASE_URL", "http://127.0.0.1:8000"),

		// Defaults raised from this backend's original hardcoded 25/10/5m
		// (still fine for early/single-tenant testing) to give a
		// multi-tenant SaaS deployment more headroom out of the box —
		// see the Config.DBMaxOpenConns docblock for how to size these
		// properly for a real deployment instead of relying on the
		// default.
		DBMaxOpenConns:           getEnvInt("DB_MAX_OPEN_CONNS", 50),
		DBMaxIdleConns:           getEnvInt("DB_MAX_IDLE_CONNS", 15),
		DBConnMaxLifetimeMinutes: getEnvInt("DB_CONN_MAX_LIFETIME_MINUTES", 5),
	}

	cfg.validate()

	return cfg
}

func (c *Config) validate() {
	if c.SecretAPIKey == "" {
		log.Fatal("config: SECRET_API_KEY is required")
	}
	if c.DBName == "" {
		log.Fatal("config: DB_NAME is required")
	}
}

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok && value != "" {
		return value
	}
	return fallback
}

// getEnvInt mirrors getEnv for integer-valued settings (pool sizes, etc.)
// — an unset or unparseable value silently falls back rather than
// failing startup, since these are tuning knobs, not required secrets.
func getEnvInt(key string, fallback int) int {
	value, ok := os.LookupEnv(key)
	if !ok || value == "" {
		return fallback
	}

	parsed, err := strconv.Atoi(value)
	if err != nil {
		log.Printf("config: %s=%q is not a valid integer, using default %d", key, value, fallback)
		return fallback
	}

	return parsed
}
