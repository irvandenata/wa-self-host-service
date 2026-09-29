package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	Port              int
	APIKey            string
	HMACSecret        string
	AdminAPIKey       string
	RedisURL          string
	QueueMax          int
	QueueDuration     time.Duration
	WorkerConcurrency int
	MaxAttempts       int
	SessionDB         string
}

func Load() (Config, error) {
	c := Config{
		APIKey:      os.Getenv("API_KEY"),
		HMACSecret:  os.Getenv("API_HMAC_SECRET"),
		AdminAPIKey: os.Getenv("ADMIN_API_KEY"),
		RedisURL:    env("REDIS_URL", "redis://localhost:6379"),
		SessionDB:   env("WA_SESSION_DB", "./data/whatsapp.db"),
	}
	var err error
	if c.Port, err = number("PORT", 8080); err != nil {
		return c, err
	}
	if c.QueueMax, err = number("WA_QUEUE_MAX", 20); err != nil {
		return c, err
	}
	durationMS, err := number("WA_QUEUE_DURATION_MS", 10000)
	if err != nil {
		return c, err
	}
	c.QueueDuration = time.Duration(durationMS) * time.Millisecond
	if c.WorkerConcurrency, err = number("WA_WORKER_CONCURRENCY", 1); err != nil {
		return c, err
	}
	if c.MaxAttempts, err = number("WA_MAX_ATTEMPTS", 3); err != nil {
		return c, err
	}
	if c.APIKey == "" {
		return c, fmt.Errorf("API_KEY required")
	}
	return c, nil
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func number(key string, fallback int) (int, error) {
	value := os.Getenv(key)
	if value == "" {
		return fallback, nil
	}
	n, err := strconv.Atoi(value)
	if err != nil || n < 1 {
		return 0, fmt.Errorf("%s must be a positive integer", key)
	}
	return n, nil
}
