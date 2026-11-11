package main

import (
	"context"
	cryptorand "crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"math/rand"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"time"

	"github.com/git-sudo-404/chronos-telemetry-pipeline/services/mock-generator/config"
	"github.com/git-sudo-404/chronos-telemetry-pipeline/services/mock-generator/internal/network"
	"github.com/git-sudo-404/chronos-telemetry-pipeline/services/mock-generator/internal/simulator"
	"github.com/git-sudo-404/chronos-telemetry-pipeline/services/mock-generator/internal/telemetry"
)

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	cfg := config.NewConfig()
	cfg.ParseConfig()

	shardCount := cfg.StateWorkerCount
	if shardCount <= 0 {
		shardCount = runtime.NumCPU()
	}
	if shardCount < 1 {
		shardCount = 1
	}
	workerCount := cfg.HTTPWorkerCount
	if workerCount <= 0 {
		workerCount = runtime.NumCPU() * 4
	}
	queueCapacity := cfg.EventQueueSize
	if queueCapacity <= 0 {
		queueCapacity = 10000
	}

	channels := make([]chan telemetry.ImmutableSnapshot, shardCount)
	shards := make([]*simulator.ShardWorker, shardCount)
	for i := 0; i < shardCount; i++ {
		channels[i] = make(chan telemetry.ImmutableSnapshot, queueCapacity)
		shards[i] = simulator.NewShardWorker(i, channels[i])
	}

	assignments := make([][]*telemetry.VehicleState, shardCount)
	for vehicleID := cfg.VehicleStartID; vehicleID <= cfg.VehicleEndID; vehicleID++ {
		vin := fmt.Sprintf("VIN-MOCK-%08d", vehicleID)
		state, err := newVehicleState(vin, cfg)
		if err != nil {
			slog.Error("vehicle initialization failed", slog.String("vehicle_id", vin), slog.Any("error", err))
			os.Exit(1)
		}
		shardIndex := stableShardIndex(vin, shardCount)
		assignments[shardIndex] = append(assignments[shardIndex], state)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	senderDone := network.StartSenderPool(ctx, channels, workerCount, cfg.GatewayURL)

	var shardWG sync.WaitGroup
	for _, shard := range shards {
		shardWG.Add(1)
		go func(shard *simulator.ShardWorker) {
			defer shardWG.Done()
			if err := shard.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
				slog.Error("shard stopped with error", slog.Int("shard_id", shard.ShardID), slog.Any("error", err))
			}
		}(shard)
	}
	for shardIndex, states := range assignments {
		for _, state := range states {
			if err := shards[shardIndex].AddVehicle(ctx, state); err != nil {
				slog.Error("vehicle assignment failed", slog.String("vehicle_id", state.VIN), slog.Int("shard_id", shardIndex), slog.Any("error", err))
				stop()
				shardWG.Wait()
				for _, channel := range channels {
					close(channel)
				}
				<-senderDone
				return
			}
		}
	}

	nodeID, err := os.Hostname()
	if err != nil || nodeID == "" {
		nodeID = "unknown"
	}
	slog.Info("mock generator started",
		slog.String("node_id", nodeID),
		slog.Int("vehicle_start_id", cfg.VehicleStartID),
		slog.Int("vehicle_end_id", cfg.VehicleEndID),
		slog.Int("shard_count", shardCount),
		slog.Int("sender_workers", workerCount),
	)
	<-ctx.Done()
	stop()

	shardWG.Wait()
	for _, channel := range channels {
		close(channel)
	}
	<-senderDone
	slog.Info("mock generator stopped after draining sender queues", slog.String("node_id", nodeID))
}

func newVehicleState(vin string, cfg *config.Config) (*telemetry.VehicleState, error) {
	var seedBytes [8]byte
	if _, err := cryptorand.Read(seedBytes[:]); err != nil {
		return nil, fmt.Errorf("generate random seed: %w", err)
	}
	seed := int64(binary.LittleEndian.Uint64(seedBytes[:]))
	rng := rand.New(rand.NewSource(seed))
	state := &telemetry.VehicleState{
		VIN:                 vin,
		SequenceNumber:      0,
		NextEventTimes:      make(map[telemetry.TelemetryEventType]time.Time),
		EventIntervals:      make(map[telemetry.TelemetryEventType]time.Duration),
		SeededRand:          rng,
		SchemaVersion:       1,
		Source:              "mock-generator",
		IgnitionOn:          true,
		Latitude:            -60 + rng.Float64()*120,
		Longitude:           -180 + rng.Float64()*360,
		SpeedKmh:            20 + rng.Float64()*90,
		HeadingDir:          rng.Float64() * 360,
		FuelLevelPct:        30 + rng.Float64()*70,
		VoltageV:            14.1,
		CurrentA:            floatPointer(10),
		TemperatureC:        floatPointer(30),
		StateOfChargePct:    floatPointer(75),
		OdometerKm:          floatPointer(rng.Float64() * 200000),
		FuelConsumedTotalL:  floatPointer(rng.Float64() * 10000),
		CoolantTempC:        80 + rng.Float64()*10,
		DriftProbabilityPct: cfg.DriftProbabilityPct,
		DriftDurationSec:    cfg.DriftDurationSec,
		MaxBufferSize:       1000,
	}
	state.EventIntervals[telemetry.TypeGPS] = time.Duration(cfg.GpsFreqSec) * time.Second
	state.EventIntervals[telemetry.TypeEngine] = time.Duration(cfg.EngineFreqSec) * time.Second
	state.EventIntervals[telemetry.TypeFuel] = time.Duration(cfg.FuelFreqSec) * time.Second
	state.EventIntervals[telemetry.TypeBattery] = time.Duration(cfg.BatteryFreqSec) * time.Second
	state.EventIntervals[telemetry.TypeDiagnostic] = time.Duration(cfg.DiagnosticFreqSec) * time.Second
	return state, nil
}

func stableShardIndex(vin string, shardCount int) int {
	h := fnv.New64a()
	_, _ = h.Write([]byte(vin))
	return int(h.Sum64() % uint64(shardCount))
}

func floatPointer(value float64) *float64 {
	return &value
}
