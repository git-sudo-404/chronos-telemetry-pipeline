/*
 * MIT License
 *
 * Copyright (c) 2026 git-sudo-404
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * Of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * Copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * Copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING, BUT NOT LIMITED TO, THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES, OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package config

import (
	"log/slog"
	"os"
	"runtime"
	"strconv"
)

type Config struct {
	GatewayURL             string
	VehicleStartID         int
	VehicleEndID           int
	GpsFreqSec             int
	EngineFreqSec          int
	FuelFreqSec            int
	BatteryFreqSec         int
	DiagnosticFreqSec      int
	DeadzoneProbabilityPct int
	DeadzoneDurationSec    int
	DriftProbabilityPct    int
	DriftDurationSec       int
	TargetEventsPerSec     int
	StateWorkerCount       int
	HTTPWorkerCount        int
	EventQueueSize         int
}

func NewConfig() *Config {
	return &Config{}
}

func (c *Config) ParseConfig() {
	c.GatewayURL = getEnv("GATEWAY_URL", "http://localhost:8080/api/v1/telemetry/ingest")
	c.VehicleStartID = getEnvAsInt("VEHICLE_START_ID", 1)
	c.VehicleEndID = getEnvAsInt("VEHICLE_END_ID", 1000)
	c.GpsFreqSec = getEnvAsInt("GPS_FREQ_SEC", 2)
	c.EngineFreqSec = getEnvAsInt("ENGINE_FREQ_SEC", 5)
	c.FuelFreqSec = getEnvAsInt("FUEL_FREQ_SEC", 10)
	c.BatteryFreqSec = getEnvAsInt("BATTERY_FREQ_SEC", 10)
	c.DiagnosticFreqSec = getEnvAsInt("DIAGNOSTIC_FREQ_SEC", 30)
	c.DeadzoneProbabilityPct = getEnvAsInt("DEADZONE_PROBABILITY_PCT", 10)
	c.DeadzoneDurationSec = getEnvAsInt("DEADZONE_DURATION_SEC", 15)
	c.DriftProbabilityPct = getEnvAsInt("DRIFT_PROBABILITY_PCT", 5)
	c.DriftDurationSec = getEnvAsInt("DRIFT_DURATION_SEC", 3)
	c.TargetEventsPerSec = getEnvAsInt("TARGET_EVENTS_PER_SEC", 2000)
	c.StateWorkerCount = getEnvAsInt("STATE_WORKER_COUNT", runtime.NumCPU())
	c.HTTPWorkerCount = getEnvAsInt("HTTP_WORKER_COUNT", runtime.NumCPU()*4)
	c.EventQueueSize = getEnvAsInt("EVENT_QUEUE_SIZE", 10000)
}

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	slog.Warn("environment variable missing, using fallback", slog.String("variable", key), slog.String("fallback_value", fallback))
	return fallback
}

func getEnvAsInt(key string, fallback int) int {
	value, ok := os.LookupEnv(key)
	if !ok {
		slog.Warn("environment variable missing, using fallback", slog.String("variable", key), slog.Int("fallback_value", fallback))
		return fallback
	}

	parsed, err := strconv.Atoi(value)
	if err != nil {
		slog.Warn("invalid integer environment value; using fallback", slog.String("variable", key), slog.String("value", value), slog.Int("fallback_value", fallback), slog.Any("error", err))
		return fallback
	}
	return parsed
}
