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

package network

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/git-sudo-404/chronos-telemetry-pipeline/services/mock-generator/internal/telemetry"
	vehiclev1 "github.com/git-sudo-404/chronos-telemetry-pipeline/services/telemetry-schemas/gen/go/vehicle/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var SharedHTTPClient = &http.Client{
	Timeout: 30 * time.Second,
	Transport: &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		MaxIdleConns:          1000,
		MaxIdleConnsPerHost:   1000,
		MaxConnsPerHost:       1000,
		IdleConnTimeout:       90 * time.Second,
		ForceAttemptHTTP2:     true,
		ExpectContinueTimeout: 1 * time.Second,
	},
}

func StartSenderPool(
	ctx context.Context,
	channels []chan telemetry.ImmutableSnapshot,
	workerCount int,
	gatewayURL string,
) <-chan struct{} {
	done := make(chan struct{})
	if workerCount < 1 {
		slog.Error("invalid sender worker count", slog.Int("worker_count", workerCount))
		close(done)
		return done
	}
	if gatewayURL == "" {
		slog.Error("gateway URL is empty")
		close(done)
		return done
	}

	fanIn := make(chan telemetry.ImmutableSnapshot, len(channels))
	var forwarders sync.WaitGroup
	for _, source := range channels {
		if source == nil {
			continue
		}
		forwarders.Add(1)
		go func(source <-chan telemetry.ImmutableSnapshot) {
			defer forwarders.Done()
			for snapshot := range source {
				fanIn <- snapshot
			}
		}(source)
	}

	go func() {
		forwarders.Wait()
		close(fanIn)
	}()

	var workers sync.WaitGroup
	workers.Add(workerCount)
	for workerID := 0; workerID < workerCount; workerID++ {
		go func(id int) {
			defer workers.Done()
			for snapshot := range fanIn {
				if err := sendSnapshot(context.Background(), gatewayURL, snapshot); err != nil {
					status := 0
					var statusErr *httpStatusError
					if errors.As(err, &statusErr) {
						status = statusErr.statusCode
					}
					eventType, _ := eventTypeString(snapshot.EventType)
					slog.Error("gateway request failed",
						slog.Int("worker_id", id),
						slog.String("vehicle_id", snapshot.VehicleID),
						slog.String("event_type", eventType),
						slog.Int("http_status", status),
						slog.String("error_message", err.Error()),
						slog.Int("retry_attempt", 0),
					)
				}
			}
		}(workerID)
	}
	go func() {
		workers.Wait()
		close(done)
	}()
	_ = ctx
	return done
}

func sendSnapshot(ctx context.Context, gatewayURL string, snapshot telemetry.ImmutableSnapshot) error {
	eventType, ok := eventTypeString(snapshot.EventType)
	if !ok {
		return errUnknownEventType
	}
	message, err := protobufMessage(snapshot)
	if err != nil {
		return err
	}
	payload, err := proto.Marshal(message)
	if err != nil {
		return err
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodPost, gatewayURL, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/x-protobuf")
	request.Header.Set("X-Vehicle-ID", snapshot.VehicleID)
	request.Header.Set("X-Event-Type", eventType)
	request.Header.Set("X-Timestamp-Delta", strconv.FormatInt(int64(time.Since(snapshot.Timestamp).Seconds()), 10))

	response, err := SharedHTTPClient.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if _, err := io.Copy(io.Discard, response.Body); err != nil {
		return err
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return &httpStatusError{status: response.Status, statusCode: response.StatusCode}
	}
	return nil
}

func protobufMessage(snapshot telemetry.ImmutableSnapshot) (proto.Message, error) {
	state := snapshot.StateSnapshot
	if state == nil {
		return nil, errMissingState
	}
	eventType, ok := eventTypeString(snapshot.EventType)
	if !ok {
		return nil, errUnknownEventType
	}
	metadata := &vehiclev1.EventMetadata{
		EventId:       snapshot.EventID,
		EventType:     eventType,
		SchemaVersion: state.SchemaVersion,
		Source:        state.Source,
		Timestamp:     timestamppb.New(snapshot.Timestamp),
		EntityId:      snapshot.VehicleID,
		IsBuffered:    snapshot.IsBuffered,
	}

	switch snapshot.EventType {
	case telemetry.TypeGPS:
		return &vehiclev1.VehicleGpsEvent{
			Metadata:     metadata,
			Latitude:     state.Latitude,
			Longitude:    state.Longitude,
			AltitudeM:    state.AltitudeM,
			SpeedKmh:     state.SpeedKmh,
			HeadingDir:   state.HeadingDir,
			GpsAccuracyM: state.GpsAccuracyM,
			OdometerKm:   state.OdometerKm,
			IgnitionOn:   state.IgnitionOn,
		}, nil
	case telemetry.TypeFuel:
		return &vehiclev1.VehicleFuelEvent{
			Metadata:           metadata,
			FuelLevelPct:       state.FuelLevelPct,
			FuelRateLph:        state.FuelRateLph,
			FuelConsumedTotalL: state.FuelConsumedTotalL,
		}, nil
	case telemetry.TypeBattery:
		return &vehiclev1.VehicleBatteryEvent{
			Metadata:         metadata,
			VoltageV:         state.VoltageV,
			CurrentA:         state.CurrentA,
			TemperatureC:     state.TemperatureC,
			StateOfChargePct: state.StateOfChargePct,
		}, nil
	case telemetry.TypeEngine:
		return &vehiclev1.VehicleEngineTelemetryEvent{
			Metadata:      metadata,
			Rpm:           state.RPM,
			CoolantTempC:  state.CoolantTempC,
			EngineLoadPct: state.EngineLoadPct,
		}, nil
	case telemetry.TypeDiagnostic:
		return &vehiclev1.VehicleDiagnosticEvent{
			Metadata:    metadata,
			Code:        state.DiagnosticCode,
			Severity:    state.DiagnosticSeverity,
			Description: state.DiagnosticDescription,
		}, nil
	default:
		return nil, errUnknownEventType
	}
}

func eventTypeString(eventType telemetry.TelemetryEventType) (string, bool) {
	switch eventType {
	case telemetry.TypeGPS, telemetry.TypeFuel, telemetry.TypeBattery, telemetry.TypeEngine, telemetry.TypeDiagnostic:
		return eventType.String(), true
	default:
		return "", false
	}
}

type httpStatusError struct {
	status     string
	statusCode int
}

func (e *httpStatusError) Error() string { return "gateway returned " + e.status }

var (
	errMissingState     = errors.New("snapshot has no state")
	errUnknownEventType = errors.New("snapshot has unknown event type")
)
