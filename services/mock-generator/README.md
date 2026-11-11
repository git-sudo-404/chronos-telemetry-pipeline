# Chronos High-Throughput Vehicle Telemetry Generator

A highly parallel, sharded single-writer simulation engine designed to generate and stream 100,000+ binary Protobuf telemetry events per second per node under extreme network latency and cellular outage scenarios.

The throughput figure is an engineering target; validate capacity against your hardware, gateway, and network with representative load tests.

## Core features

- **Sharded single-writer architecture:** Each vehicle state belongs to one shard, avoiding shared-state mutexes in the simulation path.
- **Low-overhead priority heap scheduling:** Shards sleep until the next event is due instead of continuously scanning every vehicle.
- **Multi-stage optimized Docker layout:** A static Go binary is built separately from the minimal Alpine runtime image and runs as an unprivileged user.
- **Distributed horizontal scale:** Assign disjoint vehicle ID ranges to nodes; each node hashes vehicle IDs to local shards.
- **Bounded backpressure:** Per-shard channels and per-vehicle offline buffers have capacity limits. Slow senders naturally slow event production.
- **Binary Protobuf transport:** Event snapshots are mapped to generated telemetry schema messages and posted to the gateway.

## Environment configuration

Configuration is read from process environment variables. The `.env` and `example.env` files are examples; the Go process does not load them automatically. Export values in your shell or supply them through Compose or your runtime platform. Missing variables are logged and use the defaults below.

| Variable | Default | Purpose |
|---|---:|---|
| `GATEWAY_URL` | `http://localhost:8080/api/v1/telemetry/ingest` | Telemetry ingestion endpoint |
| `VEHICLE_START_ID` | `1` | First vehicle ID assigned to this node |
| `VEHICLE_END_ID` | `1000` | Last vehicle ID assigned to this node |
| `GPS_FREQ_SEC` | `2` | GPS event interval |
| `ENGINE_FREQ_SEC` | `5` | Engine event interval |
| `FUEL_FREQ_SEC` | `10` | Fuel event interval |
| `BATTERY_FREQ_SEC` | `10` | Battery event interval |
| `DIAGNOSTIC_FREQ_SEC` | `30` | Diagnostic event interval |
| `DEADZONE_PROBABILITY_PCT` | `10` | Configured probability for cellular outage simulation |
| `DEADZONE_DURATION_SEC` | `15` | Configured outage duration |
| `DRIFT_PROBABILITY_PCT` | `5` | Probability that an active vehicle's event is delayed |
| `DRIFT_DURATION_SEC` | `3` | Maximum configured event delay |
| `TARGET_EVENTS_PER_SEC` | `2000` | Per-node target rate setting |
| `STATE_WORKER_COUNT` | `runtime.NumCPU()` | Number of shard workers |
| `HTTP_WORKER_COUNT` | `runtime.NumCPU() * 4` | Number of sender workers |
| `EVENT_QUEUE_SIZE` | `10000` | Capacity of each shard's outbound channel |

Drift probability and duration are applied to vehicle state during startup. Shard APIs support changing offline state and replaying its buffer. The startup loop does not currently schedule outages from `DEADZONE_PROBABILITY_PCT` or `DEADZONE_DURATION_SEC` automatically.

## Local integration run

From this package directory, run with the default environment configuration:

```sh
cd services/mock-generator
go run main.go
```

To use the sample values locally, export them before launching:

```sh
set -a
source example.env
set +a
go run main.go
```

Run the five-node scale profile from the repository root:

```sh
docker compose -f deployments/docker/docker-compose.simulators.yml up --build
```

The Compose profile assigns non-overlapping vehicle ranges from 1 through 10,000 and uses host networking to reach the gateway at `localhost:8080`. Ensure the gateway is listening before starting the generators.

## Concurrency architecture

```text
Vehicle ID range
      │
      ├── stable VIN hash → one shard owner per vehicle
      ▼
┌───────────────────────────────┐
│ Shard workers                 │  Fixed count: STATE_WORKER_COUNT
│ Own state, evolve physics,     │
│ schedule due events via heap  │
└──────────────┬────────────────┘
               │ immutable Protobuf-ready snapshots
               ▼
     bounded channel per shard     Capacity: EVENT_QUEUE_SIZE
               │
               ▼
       bounded fan-in queue
               │
               ▼
┌───────────────────────────────┐
│ Sender workers                │  Fixed count: HTTP_WORKER_COUNT
│ Marshal Protobuf and POST     │
│ through shared HTTP transport │
└──────────────┬────────────────┘
               ▼
          Edge gateway
```

When downstream sending slows, sender workers drain more slowly, shard channels fill, and shard workers block on channel sends. This bounds queued event memory and propagates backpressure to simulation. Offline snapshots are buffered per vehicle up to `MaxBufferSize`; overflow drops the oldest snapshot and logs a warning. Delayed drift snapshots retain their original event timestamps.

On `SIGINT` or `SIGTERM`, shard loops stop, their channels close, and the sender pool drains queued snapshots before the process exits. Requests have a 30-second client timeout; send failures are logged and are not retried by the sender pool.
