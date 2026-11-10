# chronos-telemetry-pipeline
A highly resilient, distributed IoT telemetry ingestion engine written in Go. Mitigates downstream time-series data drift and database out-of-order write bottlenecks via a decoupled, dual-lane Hot/Cold pass-through architecture.
