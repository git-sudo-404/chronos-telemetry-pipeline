// Package telemetry builds binary vehicle.v1 frames for the simulator.
package telemetry

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	mathrand "math/rand/v2"
	"time"

	vehiclev1 "github.com/git-sudo-404/chronos-telemetry-pipeline/services/telemetry-schemas/gen/go/vehicle/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	EventGPS        = "GPS"
	EventFuel       = "FUEL"
	EventEngine     = "ENGINE"
	EventBattery    = "BATTERY"
	EventDiagnostic = "DIAGNOSTIC"
)

func BuildGpsEvent(vin string, isBuffered bool) ([]byte, error) {
	lat, lon := 37.7749+(mathrand.Float64()-0.5)*0.1, -122.4194+(mathrand.Float64()-0.5)*0.1
	altitude, accuracy := 10+mathrand.Float64()*80, 2+mathrand.Float64()*8
	odometer := 10000 + mathrand.Float64()*200000
	event := &vehiclev1.VehicleGpsEvent{
		Metadata: metadata(vin, EventGPS, isBuffered), Latitude: lat, Longitude: lon,
		AltitudeM: &altitude, SpeedKmh: mathrand.Float64() * 110,
		HeadingDir: mathrand.Float64() * 360, GpsAccuracyM: &accuracy,
		OdometerKm: &odometer, IgnitionOn: mathrand.IntN(10) != 0,
	}
	return marshal(event)
}

func BuildFuelEvent(vin string, isBuffered bool) ([]byte, error) {
	rate, consumed := mathrand.Float64()*18, mathrand.Float64()*5000
	return marshal(&vehiclev1.VehicleFuelEvent{
		Metadata: metadata(vin, EventFuel, isBuffered), FuelLevelPct: mathrand.Float64() * 100,
		FuelRateLph: &rate, FuelConsumedTotalL: &consumed,
	})
}

func BuildEngineEvent(vin string, isBuffered bool) ([]byte, error) {
	return marshal(&vehiclev1.VehicleEngineTelemetryEvent{
		Metadata: metadata(vin, EventEngine, isBuffered), Rpm: 650 + mathrand.Float64()*2800,
		CoolantTempC: 75 + mathrand.Float64()*25, EngineLoadPct: mathrand.Float64() * 100,
	})
}

func BuildBatteryEvent(vin string, isBuffered bool) ([]byte, error) {
	current, temp, charge := (mathrand.Float64()-0.5)*80, 15+mathrand.Float64()*45, 20+mathrand.Float64()*80
	return marshal(&vehiclev1.VehicleBatteryEvent{
		Metadata: metadata(vin, EventBattery, isBuffered), VoltageV: 12 + mathrand.Float64()*2,
		CurrentA: &current, TemperatureC: &temp, StateOfChargePct: &charge,
	})
}

func BuildDiagnosticEvent(vin string, isBuffered bool) ([]byte, error) {
	codes := [...]string{"P0420", "P0300", "P0171", "P0128"}
	return marshal(&vehiclev1.VehicleDiagnosticEvent{
		Metadata: metadata(vin, EventDiagnostic, isBuffered), Code: codes[mathrand.IntN(len(codes))],
		Severity:    vehiclev1.DiagnosticSeverity_DIAGNOSTIC_SEVERITY_WARNING,
		Description: "Simulated vehicle diagnostic event",
	})
}

func metadata(vin, eventType string, isBuffered bool) *vehiclev1.EventMetadata {
	return &vehiclev1.EventMetadata{
		EventId: newEventID(), EventType: eventType, SchemaVersion: 1,
		Source: "mock-generator", Timestamp: timestamppb.New(time.Now().UTC()),
		EntityId: vin, IsBuffered: isBuffered,
	}
}

func marshal(message proto.Message) ([]byte, error) { return proto.Marshal(message) }

func newEventID() string {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		// The fallback still provides uniqueness for local simulation if the OS
		// random source is unavailable; it is not used as a security token.
		return fmt.Sprintf("%x-%x", time.Now().UnixNano(), mathrand.Uint64())
	}
	id[6] = (id[6] & 0x0f) | 0x40
	id[8] = (id[8] & 0x3f) | 0x80
	encoded := make([]byte, 36)
	hex.Encode(encoded[0:8], id[0:4])
	encoded[8] = '-'
	hex.Encode(encoded[9:13], id[4:6])
	encoded[13] = '-'
	hex.Encode(encoded[14:18], id[6:8])
	encoded[18] = '-'
	hex.Encode(encoded[19:23], id[8:10])
	encoded[23] = '-'
	hex.Encode(encoded[24:36], id[10:16])
	return string(encoded)
}
