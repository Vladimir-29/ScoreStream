// Package config loads application configuration from environment variables.
package config

import (
	"fmt"
	"os"
	"strings"
)

// Config holds the configuration shared by the API and worker binaries.
type Config struct {
	HTTPAddr     string
	KafkaBrokers []string
	KafkaTopic   string
	KafkaGroupID string
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// Load reads configuration from the environment, using local development
// defaults when variables are unset. It returns an error if a required
// value ends up empty.
func Load() (Config, error) {
	cfg := Config{
		HTTPAddr:     env("HTTP_ADDR", ":8080"),
		KafkaBrokers: strings.Split(env("KAFKA_BROKERS", "localhost:9092"), ","),
		KafkaTopic:   env("KAFKA_TOPIC", "score-events"),
		KafkaGroupID: env("KAFKA_GROUP_ID", "scorestream-workers"),
	}
	if cfg.HTTPAddr == "" {
		return Config{}, fmt.Errorf("HTTP_ADDR must not be empty")
	}
	if len(cfg.KafkaBrokers) == 0 {
		return Config{}, fmt.Errorf("KAFKA_BROKERS must not be empty")
	}
	for _, b := range cfg.KafkaBrokers {
		if strings.TrimSpace(b) == "" {
			return Config{}, fmt.Errorf("KAFKA_BROKERS contains an empty broker")
		}
	}
	if cfg.KafkaTopic == "" {
		return Config{}, fmt.Errorf("KAFKA_TOPIC must not be empty")
	}
	if cfg.KafkaGroupID == "" {
		return Config{}, fmt.Errorf("KAFKA_GROUP_ID must not be empty")
	}
	return cfg, nil
}
