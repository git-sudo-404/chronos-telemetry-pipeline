// Package config loads the simulator's fleet profile from environment variables.
package config

import (
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
)

// SimulatorConfig is a value snapshot of the simulator process environment.
type SimulatorConfig struct {
	GatewayURL             string
	VehicleStartID         int
	VehicleEndID           int
	GpsFrequencySec        int
	EngineFrequencySec     int
	FuelFrequencySec       int
	BatteryFrequencySec    int
	DiagnosticFrequencySec int
	DeadzoneProbabilityPct int
	DeadzoneDurationSec    int
}

// Load reads and validates a configuration snapshot exclusively from env vars.
func Load() (SimulatorConfig, error) {
	cfg := SimulatorConfig{GatewayURL: envOrDefault("GATEWAY_URL", "http://localhost:8080/api/v1/telemetry/ingest")}
	values := []struct {
		key string
		dst *int
		def int
	}{
		{"VEHICLE_START_ID", &cfg.VehicleStartID, 1},
		{"VEHICLE_END_ID", &cfg.VehicleEndID, 1000},
		{"GPS_FREQUENCY_SEC", &cfg.GpsFrequencySec, 2},
		{"ENGINE_FREQUENCY_SEC", &cfg.EngineFrequencySec, 5},
		{"FUEL_FREQUENCY_SEC", &cfg.FuelFrequencySec, 10},
		{"BATTERY_FREQUENCY_SEC", &cfg.BatteryFrequencySec, 10},
		{"DIAGNOSTIC_FREQUENCY_SEC", &cfg.DiagnosticFrequencySec, 30},
		{"DEADZONE_PROBABILITY_PCT", &cfg.DeadzoneProbabilityPct, 10},
		{"DEADZONE_DURATION_SEC", &cfg.DeadzoneDurationSec, 15},
	}
	for _, item := range values {
		value, err := intEnv(item.key, item.def)
		if err != nil {
			return SimulatorConfig{}, err
		}
		*item.dst = value
	}

	gateway, err := url.ParseRequestURI(cfg.GatewayURL)
	if err != nil || gateway.Scheme == "" || gateway.Host == "" {
		return SimulatorConfig{}, fmt.Errorf("GATEWAY_URL must be an absolute URL")
	}
	if cfg.VehicleStartID < 0 || cfg.VehicleEndID < cfg.VehicleStartID {
		return SimulatorConfig{}, fmt.Errorf("VEHICLE_END_ID must be greater than or equal to non-negative VEHICLE_START_ID")
	}
	if cfg.GpsFrequencySec < 1 || cfg.EngineFrequencySec < 1 || cfg.FuelFrequencySec < 1 || cfg.BatteryFrequencySec < 1 || cfg.DiagnosticFrequencySec < 1 {
		return SimulatorConfig{}, fmt.Errorf("all event frequencies must be at least 1 second")
	}
	if cfg.DeadzoneProbabilityPct < 0 || cfg.DeadzoneProbabilityPct > 100 {
		return SimulatorConfig{}, fmt.Errorf("DEADZONE_PROBABILITY_PCT must be between 0 and 100")
	}
	if cfg.DeadzoneDurationSec < 1 {
		return SimulatorConfig{}, fmt.Errorf("DEADZONE_DURATION_SEC must be at least 1")
	}
	return cfg, nil
}

func intEnv(key string, fallback int) (int, error) {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer: %w", key, err)
	}
	return parsed, nil
}

func envOrDefault(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}
