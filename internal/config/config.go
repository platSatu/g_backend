package config

import (
	"log"
	"os"

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
