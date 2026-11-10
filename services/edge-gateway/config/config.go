// Package config loads the edge gateway's Twelve-Factor environment config.
package config

import (
	"fmt"
	"os"
	"strings"
)

// Config is a snapshot of the gateway's process environment.
type Config struct {
	ServerPort   string
	KafkaBrokers []string
	S3BucketName string
}

// Load reads configuration exclusively from environment variables.
func Load() (Config, error) {
	brokers := envOrDefault("KAFKA_BROKERS", "localhost:9092")
	var parsedBrokers []string
	for _, broker := range strings.Split(brokers, ",") {
		if broker = strings.TrimSpace(broker); broker != "" {
			parsedBrokers = append(parsedBrokers, broker)
		}
	}
	if len(parsedBrokers) == 0 {
		return Config{}, fmt.Errorf("KAFKA_BROKERS must contain at least one broker")
	}

	return Config{
		ServerPort:   envOrDefault("SERVER_PORT", "8080"),
		KafkaBrokers: parsedBrokers,
		S3BucketName: envOrDefault("S3_COLD_BUCKET", "chronos-cold-lakehouse"),
	}, nil
}

func envOrDefault(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}
