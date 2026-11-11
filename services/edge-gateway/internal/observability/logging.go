package observability

import (
	"log/slog"
	"os"
)

func ConfigureJSONLogger() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug})))
}

func MalformedHeaders(clientIP string) {
	slog.Warn("request rejected",
		slog.String("client_ip", clientIP),
		slog.String("error", "malformed_headers"),
		slog.Int("http_code", 400),
	)
}

func TrafficLane(vehicleID, eventType, lane string, latencyDeltaSec int64) {
	slog.Debug("telemetry routed",
		slog.String("vehicle_id", vehicleID),
		slog.String("event_type", eventType),
		slog.String("lane", lane),
		slog.Int64("latency_delta_sec", latencyDeltaSec),
	)
}
