package simulator

import (
	"container/heap"
	"context"
	"errors"
	"log"
	"time"

	"github.com/git-sudo-404/chronos-telemetry-pipeline/services/mock-generator/internal/telemetry"
)

var ErrInvalidVehicle = errors.New("vehicle must have a VIN")

type ShardWorker struct {
	ShardID       int
	Vehicles      map[string]*telemetry.VehicleState
	OutboundQueue chan<- telemetry.ImmutableSnapshot
	NextEventHeap eventHeap

	commands      chan shardCommand
	lastEvolvedAt map[string]time.Time
	order         uint64
}

type scheduledEvent struct {
	vehicleID string
	eventType telemetry.TelemetryEventType
	dueAt     time.Time
	snapshot  *telemetry.ImmutableSnapshot
	order     uint64
}

type eventHeap []scheduledEvent

func (h eventHeap) Len() int { return len(h) }
func (h eventHeap) Less(i, j int) bool {
	if h[i].dueAt.Equal(h[j].dueAt) {
		if h[i].order != h[j].order {
			return h[i].order < h[j].order
		}
		if h[i].vehicleID == h[j].vehicleID {
			return h[i].eventType < h[j].eventType
		}
		return h[i].vehicleID < h[j].vehicleID
	}
	return h[i].dueAt.Before(h[j].dueAt)
}
func (h eventHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *eventHeap) Push(value any) {
	*h = append(*h, value.(scheduledEvent))
}
func (h *eventHeap) Pop() any {
	old := *h
	last := len(old) - 1
	value := old[last]
	*h = old[:last]
	return value
}

type shardCommand struct {
	vehicle *telemetry.VehicleState
	remove  string
	replay  string
	offline *offlineCommand
}

type offlineCommand struct {
	vehicleID string
	value     bool
}

func NewShardWorker(shardID int, outboundQueue chan<- telemetry.ImmutableSnapshot) *ShardWorker {
	return &ShardWorker{
		ShardID:       shardID,
		Vehicles:      make(map[string]*telemetry.VehicleState),
		OutboundQueue: outboundQueue,
		commands:      make(chan shardCommand, 256),
		lastEvolvedAt: make(map[string]time.Time),
	}
}

func (s *ShardWorker) AddVehicle(ctx context.Context, vehicle *telemetry.VehicleState) error {
	if vehicle == nil || vehicle.VIN == "" {
		return ErrInvalidVehicle
	}
	select {
	case s.commands <- shardCommand{vehicle: vehicle}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *ShardWorker) RemoveVehicle(ctx context.Context, vehicleID string) error {
	select {
	case s.commands <- shardCommand{remove: vehicleID}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *ShardWorker) SetOffline(ctx context.Context, vehicleID string, offline bool) error {
	select {
	case s.commands <- shardCommand{offline: &offlineCommand{vehicleID: vehicleID, value: offline}}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *ShardWorker) TriggerReplay(vehicleID string) {
	s.commands <- shardCommand{replay: vehicleID}
}

func (s *ShardWorker) Run(ctx context.Context) error {
	if s.OutboundQueue == nil {
		return errors.New("shard outbound queue is nil")
	}
	if s.Vehicles == nil {
		s.Vehicles = make(map[string]*telemetry.VehicleState)
	}
	if s.lastEvolvedAt == nil {
		s.lastEvolvedAt = make(map[string]time.Time)
	}
	heap.Init(&s.NextEventHeap)

	var timer *time.Timer
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()

	for {
		if len(s.NextEventHeap) > 0 && !s.NextEventHeap[0].dueAt.After(time.Now()) {
			if err := s.processDue(ctx, heap.Pop(&s.NextEventHeap).(scheduledEvent)); err != nil {
				return err
			}
			continue
		}

		var timerC <-chan time.Time
		if len(s.NextEventHeap) > 0 {
			delay := time.Until(s.NextEventHeap[0].dueAt)
			if delay < 0 {
				delay = 0
			}
			if timer == nil {
				timer = time.NewTimer(delay)
			} else {
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(delay)
			}
			timerC = timer.C
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case command := <-s.commands:
			if err := s.applyCommand(ctx, command); err != nil {
				return err
			}
		case <-timerC:
		}
	}
}

func (s *ShardWorker) applyCommand(ctx context.Context, command shardCommand) error {
	if command.vehicle != nil {
		vehicle := command.vehicle
		if vehicle.NextEventTimes == nil {
			vehicle.NextEventTimes = make(map[telemetry.TelemetryEventType]time.Time)
		}
		if vehicle.MaxBufferSize <= 0 {
			vehicle.MaxBufferSize = 1000
		}
		now := time.Now()
		for eventType, interval := range vehicle.EventIntervals {
			if interval <= 0 {
				continue
			}
			dueAt := vehicle.NextEventTimes[eventType]
			if dueAt.IsZero() {
				dueAt = now.Add(interval)
				vehicle.NextEventTimes[eventType] = dueAt
			}
			heap.Push(&s.NextEventHeap, scheduledEvent{
				vehicleID: vehicle.VIN,
				eventType: eventType,
				dueAt:     dueAt,
			})
		}
		s.Vehicles[vehicle.VIN] = vehicle
		s.lastEvolvedAt[vehicle.VIN] = now
		return nil
	}
	if command.remove != "" {
		delete(s.Vehicles, command.remove)
		delete(s.lastEvolvedAt, command.remove)
		return nil
	}
	if command.offline != nil {
		if vehicle := s.Vehicles[command.offline.vehicleID]; vehicle != nil {
			vehicle.IsOffline = command.offline.value
		}
		return nil
	}
	if command.replay != "" {
		return s.replay(ctx, command.replay)
	}
	return nil
}

func (s *ShardWorker) processDue(ctx context.Context, event scheduledEvent) error {
	if event.snapshot != nil {
		return s.sendSnapshot(ctx, *event.snapshot)
	}
	vehicle, ok := s.Vehicles[event.vehicleID]
	if !ok || vehicle == nil {
		return nil
	}

	lastUpdated := s.lastEvolvedAt[event.vehicleID]
	if lastUpdated.IsZero() {
		lastUpdated = event.dueAt
	}
	elapsed := event.dueAt.Sub(lastUpdated)
	if elapsed < 0 {
		elapsed = 0
	}
	vehicle.Evolve(elapsed)
	s.lastEvolvedAt[event.vehicleID] = event.dueAt
	snapshot := vehicle.CaptureSnapshot(event.eventType, vehicle.IsOffline)

	if vehicle.IsOffline {
		s.appendOfflineSnapshot(vehicle, snapshot)
	} else if s.shouldDrift(vehicle) {
		delay := time.Duration(vehicle.SeededRand.Int63n(int64(vehicle.DriftDurationSec)*int64(time.Second))) + time.Nanosecond
		s.order++
		delayed := snapshot
		heap.Push(&s.NextEventHeap, scheduledEvent{dueAt: time.Now().Add(delay), snapshot: &delayed, order: s.order})
	} else if err := s.sendSnapshot(ctx, snapshot); err != nil {
		return err
	}

	if nextTime, exists := vehicle.NextEventTimes[event.eventType]; exists {
		heap.Push(&s.NextEventHeap, scheduledEvent{
			vehicleID: event.vehicleID,
			eventType: event.eventType,
			dueAt:     nextTime,
		})
	}
	return nil
}

func (s *ShardWorker) shouldDrift(vehicle *telemetry.VehicleState) bool {
	probability := vehicle.DriftProbabilityPct
	if probability <= 0 || vehicle.DriftDurationSec <= 0 || vehicle.SeededRand == nil {
		return false
	}
	if probability > 100 {
		probability = 100
	}
	return vehicle.SeededRand.Intn(100) < probability
}

func (s *ShardWorker) appendOfflineSnapshot(vehicle *telemetry.VehicleState, snapshot telemetry.ImmutableSnapshot) {
	if vehicle.MaxBufferSize <= 0 {
		vehicle.MaxBufferSize = 1000
	}
	if len(vehicle.OfflineBuffer) >= vehicle.MaxBufferSize {
		copy(vehicle.OfflineBuffer, vehicle.OfflineBuffer[1:])
		vehicle.OfflineBuffer[len(vehicle.OfflineBuffer)-1] = telemetry.ImmutableSnapshot{}
		vehicle.OfflineBuffer = vehicle.OfflineBuffer[:len(vehicle.OfflineBuffer)-1]
		log.Printf("metric=offline_buffer_dropped_total severity=warning shard_id=%d vehicle_id=%s reason=capacity_limit",
			s.ShardID, vehicle.VIN)
	}
	vehicle.OfflineBuffer = append(vehicle.OfflineBuffer, snapshot)
}

func (s *ShardWorker) replay(ctx context.Context, vehicleID string) error {
	vehicle := s.Vehicles[vehicleID]
	if vehicle == nil {
		return nil
	}
	vehicle.IsOffline = false
	for i := range vehicle.OfflineBuffer {
		snapshot := vehicle.OfflineBuffer[i]
		snapshot.IsBuffered = true
		if err := s.sendSnapshot(ctx, snapshot); err != nil {
			return err
		}
		vehicle.OfflineBuffer[i] = telemetry.ImmutableSnapshot{}
	}
	vehicle.OfflineBuffer = nil
	return nil
}

func (s *ShardWorker) sendSnapshot(ctx context.Context, snapshot telemetry.ImmutableSnapshot) error {
	select {
	case s.OutboundQueue <- snapshot:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
