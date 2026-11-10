package main

import (
	"context"
	"fmt"
	"log"
	"os/signal"
	"sync"
	"syscall"

	"github.com/git-sudo-404/chronos-telemetry-pipeline/services/mock-generator/config"
	"github.com/git-sudo-404/chronos-telemetry-pipeline/services/mock-generator/internal/network"
	"github.com/git-sudo-404/chronos-telemetry-pipeline/services/mock-generator/internal/simulator"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("load simulator config: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	taskChan := make(chan network.TelemetryTask, 50000)
	workerDone := network.StartWorkerPool(context.Background(), taskChan, 100, cfg.GatewayURL)

	var actors sync.WaitGroup
	for id := cfg.VehicleStartID; ; id++ {
		vin := fmt.Sprintf("VIN-MOCK-%08d", id)
		actors.Add(1)
		go func() {
			defer actors.Done()
			simulator.StartVehicleActor(ctx, vin, taskChan, &cfg)
		}()
		if id == cfg.VehicleEndID {
			break
		}
	}
	log.Printf("mock generator started vehicles=%d-%d gateway=%s", cfg.VehicleStartID, cfg.VehicleEndID, cfg.GatewayURL)

	<-ctx.Done()
	actors.Wait()
	close(taskChan)
	<-workerDone
	log.Print("mock generator stopped")
}
