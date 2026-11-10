
# Technical Requirements Document (TRD): Project Chronos

## 1. System Objective & Problem Statement
Commercial connected-vehicle telemetry platforms must process massive volumes of real-time data frames (GPS, speed, engine diagnostics) transmitted over unstable cellular networks. This architecture is designed to solve two core system failures:
1. **The Telemetry Dump Surge (Network Backpressure):** When vehicles recover from cellular dead zones, they instantly flush hours of locally buffered historical packets in a massive, sub-second network dump. This spikes edge controller CPU limits and starves thread pools.
2. **The Chronological Drift (Out-of-Order Corruption):** Interleaved dumps cause historical packets to arrive out of order alongside true real-time pings. If a time-series OLAP database writes these records out of order, it triggers severe computational write thrashing and corrupts active analytical metrics.

**Project Chronos** solves this by engineering a high-throughput, non-blocking, zero-copy **Dual-Lane Ingestion Gateway** that evaluates packet age at the network edge, isolating real-time streams from historical dumps to preserve database consistency and performance.

---

## 2. Scale & Performance Targets
To validate the architecture under production-level constraints, the engine must satisfy the following metrics:
* **Throughput Capacity:** Sustain a baseline of **1,000,000 requests per minute (~16,666 events per second)** under peak simulation loads.
* **Edge Processing Latency:** Network edge execution (header sniffing, lane evaluation, and buffer handoff) must execute in **< 2 milliseconds** per transaction.
* **Hot Path Availability:** Validated real-time packets must be committed to the OLAP tier and queryable within **< 1 second** of entering the gateway.
* **Zero Data Loss Integrity:** Under simulated cluster crashes or target database failures, the edge layer must drop zero packets, degrading gracefully to fallback storage states.

---

## 3. Technology Stack Choice & Rationale

| Infrastructure Layer | Component Selection | Strategic Engineering Rationale |
| :--- | :--- | :--- |
| **Ingestion Edge Runtime** | **Go (Golang)** | Chosen over Spring Boot or Node.js. Eliminates runtime JVM memory footprints (reducing costs) and avoids annotation-scanning or JIT warm-up latency. Compiles to a single binary with a **< 10ms container boot time**, enabling microsecond Kubernetes HPA auto-scaling to absorb traffic waves instantly. |
| **Data Serialization** | **Google Protocol Buffers (Protobuf)** | Chosen over text-based JSON. Eliminates expensive CPU reflection parsing and text conversion loops. Compiles values directly into raw binary offsets from memory, operating **10x faster** with near-zero object allocation while compressing telemetry network frames by **~70%**. |
| **Transport Protocol** | **HTTP / Protobuf Stream** | Configured to process standard `application/x-protobuf` POST requests with promoted meta-headers. Bypasses the initial implementation complexity of an external MQTT broker while retaining real-world, binary pass-through routing logic. |
| **Event Stream Fabric** | **Apache Kafka** | Distributed, partitioned append-only log backbone. By using the `vehicle_id` as the message partitioning key, it enforces **strict chronological sequence ordering** per vehicle asset while permitting massive downstream consumer scale. |
| **Hot Analytics Tier** | **ClickHouse (OLAP)** | Column-oriented database engine. Utilizes a native ClickHouse Kafka Engine to parse the binary Protobuf stream directly into columnar layouts, avoiding application-layer code boundaries. Handles real-time telemetry analytics for the active **0-30 day window**. |
| **Cold Storage Layer** | **AWS S3 + Apache Iceberg** | Telemetry frames older than 30 days are tiered off to S3 as highly compressed, immutable **Apache Parquet files**. Managed via **Apache Iceberg open table schemas**, allowing serverless, low-cost SQL analytics over historical datasets using AWS Athena. |
| **DevOps Architecture** | **Terraform, Kubernetes (EKS), Ansible** | Entire infrastructure layer codified using Terraform for repeatability on AWS, orchestrated via Amazon EKS containers, and state-configured natively using Ansible playbooks. |

---

## 4. Pipeline Mechanics: Inputs, Actions, & Outputs

### 📥 The Input
The engine ingests raw binary byte payloads over HTTP POST. To maintain sub-millisecond dispatching, routing parameters are promoted to the transport layer headers:
* `X-Vehicle-ID`: The unique vehicular hardware text identifier (used as the Kafka routing key).
* `X-Timestamp-Delta`: An integer calculation tracking the true chronological age of the packet (`Current Ingestion Gateway Epoch Time` minus `Vehicle Record Generation Epoch Time`).

### ⚙️ System Actions & Core Architecture Lanes
When a payload strikes the Go execution context, the engine executes a zero-copy routing evaluation:
1. **The Hot Async Lane (`X-Timestamp-Delta < 5s`):** The raw, unparsed binary payload byte array is instantly shot into the primary Kafka Hot Topic. The main HTTP network thread exits immediately with an `HTTP 202 Accepted` status. Downstream, ClickHouse reads the topic natively.
2. **The Spill/Batch Lane (`X-Timestamp-Delta ≥ 5s`):** The engine intercepts the historical dump and routes the binary stream away from the hot Kafka topics. It pools these delayed packets into an isolated memory-buffered staging pool that periodically dumps files directly into the AWS S3 Parquet Lakehouse, keeping the live analytical database nodes fully insulated from out-of-order writes.

### 📤 The Output
* **Hot Storage:** A highly optimized, columnar time-series database matrix containing chronologically clean vehicular indices for real-time fleet operations.
* **Cold Storage:** Immutable, optimized `.parquet` data chunks grouped by date partitions (`s3://telemetry-lakehouse/year=YYYY/month=MM/day=DD/`) for long-term audit histories.

---

## 5. Architectural Guarantees & Scope Boundaries

### 🛡️ System Guarantees
* **Prism Query Integrity:** The live OLAP database is completely insulated from out-of-order write thrashing or historic log backpressure pipelines.
* **Non-Blocking Network Gate:** The entry layer functions independently of downstream database storage loops; failures or slowness in ClickHouse or Kafka will not block or drop incoming edge requests.
* **Linear Elasticity:** The stateless Go ingestion containers can scale out under Kubernetes instantly inside traffic surge parameters due to immediate runtime start execution.

### ❌ Out of Scope
* **MQTT Network Brokers:** Excluded from the execution scope to ensure engineering focus remains squarely on the distributed system processing problem rather than IoT socket network debugging.
* **User Interfaces / Dashboards:** No front-end logic or UI platforms. Performance and architectural integrity are proved exclusively via log profiles, consumer lag tools, and ClickHouse columnar execution metrics.
* **AI Agent Monitoring:** Completely isolated from this repository; deferred entirely to subsequent processing architectures.
