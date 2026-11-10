package simulator

import (
	"context"
	"fmt"
	"log"
	mathrand "math/rand/v2"
	"sync"
	"time"

	"github.com/git-sudo-404/chronos-telemetry-pipeline/services/mock-generator/config"
	"github.com/git-sudo-404/chronos-telemetry-pipeline/services/mock-generator/internal/network"
	"github.com/git-sudo-404/chronos-telemetry-pipeline/services/mock-generator/internal/telemetry"
)

type VehicleState uint8

const (
	STATE_ONLINE VehicleState = iota
	STATE_CELLULAR_DEADZONE
)

type offlineFrameMeta struct {
	eventType string
	createdAt time.Time
}

// VehicleActor owns one vehicle's timers and offline payload buffer.
type VehicleActor struct {
	VIN              string
	state            VehicleState
	mu               sync.Mutex
	offlineBuffer    [][]byte
	offlineFrameMeta []offlineFrameMeta
}

func StartVehicleActor(ctx context.Context, vin string, taskChan chan<- network.TelemetryTask, cfg *config.SimulatorConfig) {
	if cfg == nil || taskChan == nil {
		log.Printf("vehicle actor %s not started: config and task channel are required", vin)
		return
	}
	actor := &VehicleActor{VIN: vin, state: STATE_ONLINE}
	intervals := []time.Duration{
		time.Duration(cfg.GpsFrequencySec) * time.Second,
		time.Duration(cfg.EngineFrequencySec) * time.Second,
		time.Duration(cfg.FuelFrequencySec) * time.Second,
		time.Duration(cfg.BatteryFrequencySec) * time.Second,
		time.Duration(cfg.DiagnosticFrequencySec) * time.Second,
	}
	for _, interval := range intervals {
		if interval <= 0 {
			log.Printf("vehicle actor %s not started: event frequencies must be positive", vin)
			return
		}
	}
	deadzoneDuration := time.Duration(cfg.DeadzoneDurationSec) * time.Second
	if deadzoneDuration <= 0 || cfg.DeadzoneProbabilityPct < 0 || cfg.DeadzoneProbabilityPct > 100 {
		log.Printf("vehicle actor %s not started: invalid deadzone configuration", vin)
		return
	}

	gpsTicker := time.NewTicker(intervals[0])
	engineTicker := time.NewTicker(intervals[1])
	fuelTicker := time.NewTicker(intervals[2])
	batteryTicker := time.NewTicker(intervals[3])
	diagnosticTicker := time.NewTicker(intervals[4])
	deadzoneTicker := time.NewTicker(deadzoneDuration)
	defer gpsTicker.Stop()
	defer engineTicker.Stop()
	defer fuelTicker.Stop()
	defer batteryTicker.Stop()
	defer diagnosticTicker.Stop()
	defer deadzoneTicker.Stop()

	var deadzoneTimer *time.Timer
	var deadzoneTimeout <-chan time.Time
	defer func() {
		if deadzoneTimer != nil {
			deadzoneTimer.Stop()
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case <-gpsTicker.C:
			actor.emit(ctx, taskChan, telemetry.EventGPS)
		case <-engineTicker.C:
			actor.emit(ctx, taskChan, telemetry.EventEngine)
		case <-fuelTicker.C:
			actor.emit(ctx, taskChan, telemetry.EventFuel)
		case <-batteryTicker.C:
			actor.emit(ctx, taskChan, telemetry.EventBattery)
		case <-diagnosticTicker.C:
			actor.emit(ctx, taskChan, telemetry.EventDiagnostic)
		case <-deadzoneTicker.C:
			if actor.currentState() == STATE_ONLINE && mathrand.IntN(100) < cfg.DeadzoneProbabilityPct {
				actor.setState(STATE_CELLULAR_DEADZONE)
				deadzoneTimer = time.NewTimer(deadzoneDuration)
				deadzoneTimeout = deadzoneTimer.C
			}
		case <-deadzoneTimeout:
			actor.setState(STATE_ONLINE)
			deadzoneTimeout = nil
			deadzoneTimer = nil
			actor.flush(ctx, taskChan)
		}
	}
}

func (a *VehicleActor) emit(ctx context.Context, taskChan chan<- network.TelemetryTask, eventType string) {
	createdAt := time.Now()
	isOffline := a.currentState() == STATE_CELLULAR_DEADZONE
	payload, err := buildFrame(a.VIN, eventType, isOffline)
	if err != nil {
		log.Printf("vehicle actor %s failed to build %s frame: %v", a.VIN, eventType, err)
		return
	}
	if isOffline {
		a.mu.Lock()
		a.offlineBuffer = append(a.offlineBuffer, payload)
		a.offlineFrameMeta = append(a.offlineFrameMeta, offlineFrameMeta{eventType: eventType, createdAt: createdAt})
		a.mu.Unlock()
		return
	}
	a.send(ctx, taskChan, network.TelemetryTask{
		VehicleID: a.VIN, EventType: eventType, PayloadBytes: payload,
		TimestampDelta: 0,
	})
}

func (a *VehicleActor) flush(ctx context.Context, taskChan chan<- network.TelemetryTask) {
	a.mu.Lock()
	frames := a.offlineBuffer
	metadata := a.offlineFrameMeta
	a.offlineBuffer = nil
	a.offlineFrameMeta = nil
	a.mu.Unlock()

	for i, payload := range frames {
		a.send(ctx, taskChan, network.TelemetryTask{
			VehicleID: a.VIN, EventType: metadata[i].eventType, PayloadBytes: payload,
			TimestampDelta: time.Since(metadata[i].createdAt),
		})
		if ctx.Err() != nil {
			return
		}
	}
}

func (a *VehicleActor) send(ctx context.Context, taskChan chan<- network.TelemetryTask, task network.TelemetryTask) {
	select {
	case taskChan <- task:
	case <-ctx.Done():
	}
}

func (a *VehicleActor) currentState() VehicleState {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.state
}

func (a *VehicleActor) setState(state VehicleState) {
	a.mu.Lock()
	a.state = state
	a.mu.Unlock()
}

func buildFrame(vin, eventType string, buffered bool) ([]byte, error) {
	switch eventType {
	case telemetry.EventGPS:
		return telemetry.BuildGpsEvent(vin, buffered)
	case telemetry.EventFuel:
		return telemetry.BuildFuelEvent(vin, buffered)
	case telemetry.EventEngine:
		return telemetry.BuildEngineEvent(vin, buffered)
	case telemetry.EventBattery:
		return telemetry.BuildBatteryEvent(vin, buffered)
	case telemetry.EventDiagnostic:
		return telemetry.BuildDiagnosticEvent(vin, buffered)
	default:
		return nil, fmt.Errorf("unsupported telemetry event type %q", eventType)
	}
}
