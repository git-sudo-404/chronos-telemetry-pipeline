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

package telemetry

import (
	"crypto/rand"
	"fmt"
	"math"
	"time"
)

const (
	earthRadiusKm    = 6371.0
	defaultTankSizeL = 60.0
	ambientTempC     = 25.0
)

var defaultEventIntervals = map[TelemetryEventType]time.Duration{
	TypeGPS:        2 * time.Second,
	TypeFuel:       10 * time.Second,
	TypeBattery:    10 * time.Second,
	TypeEngine:     5 * time.Second,
	TypeDiagnostic: 30 * time.Second,
}

func (v *VehicleState) Evolve(elapsed time.Duration) {
	if elapsed <= 0 {
		return
	}
	dtHours := elapsed.Hours()
	dtSeconds := elapsed.Seconds()

	if !v.IgnitionOn {
		v.RPM = 0
		v.EngineLoadPct = 0
		v.SpeedKmh = 0
		v.CoolantTempC = approach(v.CoolantTempC, ambientTempC, 0.02*dtSeconds)
		v.VoltageV = approach(v.VoltageV, 12.4, 0.03*dtSeconds)
		if v.CurrentA != nil {
			*v.CurrentA = -0.05
		}
		if v.TemperatureC != nil {
			*v.TemperatureC = approach(*v.TemperatureC, ambientTempC, 0.01*dtSeconds)
		}
		if v.StateOfChargePct != nil {
			*v.StateOfChargePct = clamp(*v.StateOfChargePct-0.005*dtHours, 0, 100)
		}
		return
	}

	distanceKm := v.SpeedKmh * dtHours
	v.move(distanceKm)
	if v.OdometerKm == nil {
		v.OdometerKm = new(float64)
	}
	*v.OdometerKm += distanceKm

	loadNoise := v.randomRange(-3, 3)
	v.EngineLoadPct = clamp(15+v.SpeedKmh/160*55+loadNoise, 10, 85)
	rpmNoise := v.randomRange(-100, 100)
	v.RPM = clamp(1500+v.SpeedKmh/120*1800+v.EngineLoadPct*2+rpmNoise, 1500, 3500)
	v.CoolantTempC = approach(v.CoolantTempC, 90, 0.015*dtSeconds)

	fuelRateLph := 0.4 + v.EngineLoadPct*0.12
	if v.FuelRateLph == nil {
		v.FuelRateLph = new(float64)
	}
	*v.FuelRateLph = fuelRateLph
	fuelBurnedL := fuelRateLph * dtHours
	if v.FuelConsumedTotalL == nil {
		v.FuelConsumedTotalL = new(float64)
	}
	*v.FuelConsumedTotalL += fuelBurnedL
	v.FuelLevelPct = clamp(v.FuelLevelPct-fuelBurnedL/defaultTankSizeL*100, 0, 100)

	v.VoltageV = approach(v.VoltageV, 14.1, 0.1*dtSeconds)
	if v.CurrentA != nil {
		*v.CurrentA = 8 + v.EngineLoadPct*0.15
	}
	if v.StateOfChargePct != nil {
		*v.StateOfChargePct = clamp(*v.StateOfChargePct+0.02*dtHours, 0, 100)
	}
	if v.TemperatureC != nil {
		vibration := v.randomRange(-0.003, 0.003)
		*v.TemperatureC = approach(*v.TemperatureC, 30+v.EngineLoadPct*0.05, (0.005+vibration)*dtSeconds)
	}
}

func (v *VehicleState) CaptureSnapshot(eventType TelemetryEventType, isBuffered bool) ImmutableSnapshot {
	if v.NextEventTimes == nil {
		v.NextEventTimes = make(map[TelemetryEventType]time.Time)
	}

	eventTime, ok := v.NextEventTimes[eventType]
	if !ok || eventTime.IsZero() {
		eventTime = time.Now()
	}
	interval := defaultEventIntervals[eventType]
	if configured, ok := v.EventIntervals[eventType]; ok && configured > 0 {
		interval = configured
	} else if interval <= 0 {
		interval = time.Second
	}
	v.NextEventTimes[eventType] = eventTime.Add(interval)
	v.SequenceNumber++

	stateCopy := *v
	stateCopy.AltitudeM = cloneFloat64(v.AltitudeM)
	stateCopy.GpsAccuracyM = cloneFloat64(v.GpsAccuracyM)
	stateCopy.OdometerKm = cloneFloat64(v.OdometerKm)
	stateCopy.FuelRateLph = cloneFloat64(v.FuelRateLph)
	stateCopy.FuelConsumedTotalL = cloneFloat64(v.FuelConsumedTotalL)
	stateCopy.CurrentA = cloneFloat64(v.CurrentA)
	stateCopy.TemperatureC = cloneFloat64(v.TemperatureC)
	stateCopy.StateOfChargePct = cloneFloat64(v.StateOfChargePct)
	stateCopy.NextEventTimes = cloneEventTimes(v.NextEventTimes)
	stateCopy.EventIntervals = cloneEventIntervals(v.EventIntervals)
	stateCopy.OfflineBuffer = nil
	stateCopy.SeededRand = nil

	return ImmutableSnapshot{
		VehicleID:      v.VIN,
		SequenceNumber: v.SequenceNumber,
		EventType:      eventType,
		Timestamp:      eventTime,
		IsBuffered:     isBuffered,
		StateSnapshot:  &stateCopy,
		EventID:        newUUID(),
	}
}

func (v *VehicleState) move(distanceKm float64) {
	if distanceKm == 0 {
		return
	}
	lat1 := degreesToRadians(v.Latitude)
	lon1 := degreesToRadians(v.Longitude)
	bearing := degreesToRadians(v.HeadingDir)
	angularDistance := distanceKm / earthRadiusKm

	lat2 := math.Asin(math.Sin(lat1)*math.Cos(angularDistance) +
		math.Cos(lat1)*math.Sin(angularDistance)*math.Cos(bearing))
	lon2 := lon1 + math.Atan2(
		math.Sin(bearing)*math.Sin(angularDistance)*math.Cos(lat1),
		math.Cos(angularDistance)-math.Sin(lat1)*math.Sin(lat2),
	)
	v.Latitude = radiansToDegrees(lat2)
	v.Longitude = math.Mod(radiansToDegrees(lon2)+540, 360) - 180
}

func (v *VehicleState) randomRange(min, max float64) float64 {
	if v.SeededRand == nil {
		return 0
	}
	return min + v.SeededRand.Float64()*(max-min)
}

func approach(current, target, step float64) float64 {
	if current < target {
		return math.Min(current+step, target)
	}
	return math.Max(current-step, target)
}

func clamp(value, min, max float64) float64 {
	return math.Max(min, math.Min(value, max))
}

func degreesToRadians(degrees float64) float64 { return degrees * math.Pi / 180 }
func radiansToDegrees(radians float64) float64 { return radians * 180 / math.Pi }

func cloneFloat64(value *float64) *float64 {
	if value == nil {
		return nil
	}
	copyValue := *value
	return &copyValue
}

func cloneEventTimes(values map[TelemetryEventType]time.Time) map[TelemetryEventType]time.Time {
	copyValues := make(map[TelemetryEventType]time.Time, len(values))
	for eventType, eventTime := range values {
		copyValues[eventType] = eventTime
	}
	return copyValues
}

func cloneEventIntervals(values map[TelemetryEventType]time.Duration) map[TelemetryEventType]time.Duration {
	copyValues := make(map[TelemetryEventType]time.Duration, len(values))
	for eventType, interval := range values {
		copyValues[eventType] = interval
	}
	return copyValues
}

func newUUID() string {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		panic(fmt.Errorf("generate event UUID: %w", err))
	}
	id[6] = (id[6] & 0x0f) | 0x40
	id[8] = (id[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		id[0:4], id[4:6], id[6:8], id[8:10], id[10:16])
}
