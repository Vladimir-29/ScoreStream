// Package config загружает конфигурацию приложения из переменных окружения.
package config

import (
	"fmt"
	"os"
	"strings"
)

// Config хранит конфигурацию, общую для бинарников API и worker.
type Config struct {
	HTTPAddr     string
	DatabaseURL  string
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

// Load читает конфигурацию из окружения, используя значения по умолчанию
// для локальной разработки, если переменные не заданы. Возвращает ошибку,
// если обязательное значение в итоге пустое.
func Load() (Config, error) {
	cfg := Config{
		HTTPAddr:     env("HTTP_ADDR", ":8080"),
		DatabaseURL:  env("DATABASE_URL", "postgres://scorestream:scorestream@localhost:5432/scorestream?sslmode=disable"),
		KafkaBrokers: strings.Split(env("KAFKA_BROKERS", "localhost:9092"), ","),
		KafkaTopic:   env("KAFKA_TOPIC", "score-events"),
		KafkaGroupID: env("KAFKA_GROUP_ID", "scorestream-workers"),
	}
	if cfg.HTTPAddr == "" {
		return Config{}, fmt.Errorf("HTTP_ADDR must not be empty")
	}
	if cfg.DatabaseURL == "" {
		return Config{}, fmt.Errorf("DATABASE_URL must not be empty")
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
