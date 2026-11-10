package network

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"
)

// TelemetryTask is a serialized frame ready to be sent to the edge gateway.
type TelemetryTask struct {
	VehicleID      string
	EventType      string
	PayloadBytes   []byte
	TimestampDelta time.Duration
}

// HttpClient reuses established TCP connections across worker requests.
var HttpClient = &http.Client{
	Transport: &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 100,
		IdleConnTimeout:     90 * time.Second,
	},
	Timeout: 15 * time.Second,
}

// StartWorkerPool starts workers that drain taskChan until it closes or ctx is
// cancelled. Each request owns the task payload for the duration of the call.
func StartWorkerPool(ctx context.Context, taskChan <-chan TelemetryTask, workerCount int, gatewayURL string) <-chan struct{} {
	if workerCount < 1 {
		workerCount = 1
	}
	done := make(chan struct{})
	var workers sync.WaitGroup
	workers.Add(workerCount)
	for i := 0; i < workerCount; i++ {
		go func() {
			defer workers.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case task, ok := <-taskChan:
					if !ok {
						return
					}
					if err := dispatch(ctx, gatewayURL, task); err != nil && ctx.Err() == nil {
						log.Printf("telemetry dispatch failed vehicle=%s event=%s: %v", task.VehicleID, task.EventType, err)
					}
				}
			}
		}()
	}
	go func() {
		workers.Wait()
		close(done)
	}()
	return done
}

func dispatch(ctx context.Context, gatewayURL string, task TelemetryTask) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, gatewayURL, bytes.NewReader(task.PayloadBytes))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	request.Header.Set("Content-Type", "application/x-protobuf")
	request.Header.Set("X-Vehicle-ID", task.VehicleID)
	request.Header.Set("X-Event-Type", task.EventType)
	// Delta is encoded in fractional seconds so gateway thresholds (for example
	// live data under five seconds) compare directly to the header value.
	request.Header.Set("X-Timestamp-Delta", strconv.FormatFloat(task.TimestampDelta.Seconds(), 'f', 3, 64))

	response, err := HttpClient.Do(request)
	if err != nil {
		return fmt.Errorf("post telemetry: %w", err)
	}
	_, drainErr := io.Copy(io.Discard, response.Body)
	closeErr := response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("gateway returned status %s", response.Status)
	}
	if drainErr != nil {
		return fmt.Errorf("drain response: %w", drainErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close response: %w", closeErr)
	}
	return nil
}
