package config

import (
	"fmt"
	"log"
	"os"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// ConnectDB opens a connection pool to the "teleios" MySQL database using
// the credentials from Config and returns a ready-to-use *gorm.DB.
func ConnectDB(cfg *Config) *gorm.DB {
	dsn := fmt.Sprintf(
		"%s:%s@tcp(%s:%s)/%s?charset=utf8mb4&parseTime=True&loc=Local",
		cfg.DBUser, cfg.DBPassword, cfg.DBHost, cfg.DBPort, cfg.DBName,
	)

	// "Record not found" is expected in normal flows (e.g. checking
	// whether a user already has a wa_devices row) and shouldn't be
	// logged as an error — only genuine DB problems and slow queries are.
	gormLogger := logger.New(
		log.New(os.Stdout, "\r\n", log.LstdFlags),
		logger.Config{
			SlowThreshold:             200 * time.Millisecond,
			LogLevel:                  logger.Warn,
			IgnoreRecordNotFoundError: true,
		},
	)

	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		Logger: gormLogger,
	})
	if err != nil {
		log.Fatalf("db: failed to connect to %q: %v", cfg.DBName, err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		log.Fatalf("db: failed to access underlying sql.DB: %v", err)
	}

	// Pool size is configurable (DB_MAX_OPEN_CONNS/DB_MAX_IDLE_CONNS/
	// DB_CONN_MAX_LIFETIME_MINUTES) — see the Config.DBMaxOpenConns
	// docblock for how to size this for a real multi-tenant deployment
	// instead of trusting the default.
	sqlDB.SetMaxOpenConns(cfg.DBMaxOpenConns)
	sqlDB.SetMaxIdleConns(cfg.DBMaxIdleConns)
	sqlDB.SetConnMaxLifetime(time.Duration(cfg.DBConnMaxLifetimeMinutes) * time.Minute)

	log.Printf("db: connected to %q at %s:%s (pool: max_open=%d max_idle=%d conn_max_lifetime=%dm)",
		cfg.DBName, cfg.DBHost, cfg.DBPort, cfg.DBMaxOpenConns, cfg.DBMaxIdleConns, cfg.DBConnMaxLifetimeMinutes)

	return db
}
