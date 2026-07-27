# ClickHouse Log Pipeline (Spring Boot + Kafka + Docker + Grafana)

A high-throughput log ingestion pipeline built with Spring Boot, ClickHouse, Kafka,
and Grafana - designed to demonstrate the architecture of a production-style logging
service.

---

## 🏗️ Architecture & Structural Decisions

### High-level flow

```
                          POST /logs
                                │
                                ▼
                       LogController
                                │
                                ▼
                      IngestionService
                                │
                 ┌──────────────┴──────────────┐
                 │ mode=direct                  │ mode=kafka
                 ▼                              ▼
        BatchWriter.enqueue()            Kafka Producer
                 │                              │
                 │                        Kafka Topic "logs"
                 │                              │
                 │                        Kafka Consumer
                 │                              │
                 └──────────────┬───────────────┘
                                ▼
                       BatchWriter.enqueue()
                                │
                     BlockingQueue<LogDTO>
                                │
                Scheduled Batch Flush (1 sec)
                                │
                                ▼
                   JdbcTemplate Batch Insert
                                │
                                ▼
                          ClickHouse
                                │
                                ▼
                            Grafana
```

The REST API **never writes directly to ClickHouse.** It only validates the request
and either enqueues it directly or publishes it to Kafka. **`BatchWriter` is the
single component that writes to ClickHouse**, regardless of which transport a log
arrived through.

### Why a queue instead of a direct write

The architecture intentionally uses `Controller → Queue → BatchWriter → Database`
instead of `Controller → Database` directly. This is the core design decision of the
project:

- decouples API latency from database latency - the API returns immediately
- prevents the controller from ever blocking on a slow insert
- absorbs traffic spikes without dropping requests
- batches many small logs into fewer, larger ClickHouse inserts (flushes every
  1 second or every 1000 logs, whichever comes first), which is dramatically more
  efficient for an OLAP store than row-by-row inserts

### Kafka is a transport, not a replacement

Kafka sits **in front of** the queue as an alternative way for a log to reach
`BatchWriter.enqueue()` - it does not replace the batching mechanism:

```
Direct mode:  Controller -> Service -> BatchWriter.enqueue() -> Queue -> ClickHouse

Kafka mode:   Controller -> Service -> Producer -> Kafka topic "logs" -> Consumer
              -> BatchWriter.enqueue() -> Queue -> ClickHouse
```

Both modes converge on the same queue, the same scheduler, and the same
`JdbcTemplate.batchUpdate()` call. The Kafka consumer never writes to ClickHouse
itself - it calls the exact same `BatchWriter.enqueue()` the direct path uses. The
mode is chosen with `pipeline.ingestion.mode` (`direct` or `kafka`); in direct mode,
the Kafka beans (producer, consumer, topic config) are never even created
(`@ConditionalOnProperty`), so the app has no dependency on a running broker at all.

### Other structural decisions

- **Controller stays thin** - `LogController` only validates and delegates. No
  batching or persistence logic ever lives there.
- **Service layer stays minimal** - `IngestionService` is a pure router between the
  two transports. All batching/writing logic is concentrated in `BatchWriter`.
- **Single writer component** - all database writes go through `BatchWriter`; the
  project deliberately avoids multiple independent ClickHouse writers.
- **DTO boundary** - `LogDTO` is the ingestion payload and is never a database
  entity in disguise. New metadata (hostname, request id, region, etc.) extends
  `LogDTO` rather than introducing another DTO.
- **Configurable batching** - `pipeline.batch.size`, `pipeline.batch.flush-interval-ms`,
  and `pipeline.batch.queue-capacity` are constructor-injected into `BatchWriter` via
  `@Value`, defaulting to the original hardcoded values (1000 / 1000ms / 100000).

---

## 🚀 Features

- Accept logs via REST endpoint `/logs` (single and batch)
- Async queue + batch writer for ultra-fast ingestion
- Dual ingestion transport: write directly to the queue, or publish through Kafka -
  selectable per deployment via `pipeline.ingestion.mode`
- ClickHouse table for log storage
- Grafana dashboards (real-time log monitoring)
- Fully dockerized system, including a single-node Kafka broker (KRaft mode) and
  Kafka UI for inspecting topics and consumer lag
- A JUnit 5 test suite (44 tests) covering both ingestion modes with no external
  dependencies required to run it

---

## ⚙️ Tech Stack

- Java 17
- Spring Boot 3
- ClickHouse (JDBC)
- Spring Kafka
- Apache Kafka (KRaft mode, no ZooKeeper)
- Grafana
- Docker + Docker Compose
- JUnit 5, Mockito, MockMvc, Embedded Kafka, Awaitility

---

## 📦 Project Structure

```css
clickhouselogpipeline/
│
├── src/
│   ├── main/
│   │   ├── java/com/yashraj/clickhousepipeline/
│   │   │   ├── ClickHouseLogPipelineApplication.java
│   │   │   ├── controller/LogController.java
│   │   │   ├── dto/LogDTO.java
│   │   │   ├── service/BatchWriter.java
│   │   │   ├── service/IngestionService.java
│   │   │   ├── kafka/LogProducer.java
│   │   │   ├── kafka/LogConsumer.java
│   │   │   ├── config/ClickHouseConfig.java
│   │   │   ├── config/KafkaTopicConfig.java
│   │   │   ├── config/KafkaProducerConfig.java
│   │   │   └── config/KafkaConsumerConfig.java
│   │   └── resources/
│   │       └── application.properties
│   └── test/
│       ├── java/com/yashraj/clickhousepipeline/
│       │   ├── ClickHouseLogPipelineApplicationTests.java
│       │   ├── controller/LogControllerTest.java
│       │   ├── service/BatchWriterTest.java
│       │   ├── service/IngestionServiceTest.java
│       │   ├── kafka/LogProducerTest.java
│       │   ├── kafka/LogConsumerTest.java
│       │   ├── kafka/LogDtoSerializationTest.java
│       │   └── integration/DirectModeIngestionIntegrationTest.java
│       │   └── integration/KafkaModeIngestionIntegrationTest.java
│       └── resources/
│           └── application.properties
│
├── Dockerfile
├── docker-compose.yml
│
├── pom.xml
├── README.md
│
├── mvnw
├── mvnw.cmd
│
└── .gitignore
```

---

## 🐳 Run via Docker

### 1. Build the app:

```bash
mvn clean package
```

Add `-DskipTests` only if you specifically want to bypass the test suite - it's fast
(well under 15s) and needs no Docker or ClickHouse, so there's normally no reason to.

### 2. Start the full stack:

```bash
docker compose up --build
```

This brings up ClickHouse, Grafana, a single-node Kafka broker (KRaft mode -
`apache/kafka:3.9.1`, no ZooKeeper needed), Kafka UI, and the app itself. The
`logs` Kafka topic (3 partitions, replication factor 1) is created automatically at
application startup.

### Services

| Service       | URL |
|---------------|-------------------------|
| Spring Boot   | http://localhost:8080   |
| Grafana       | http://localhost:3000   |
| ClickHouse UI | http://localhost:8123   |
| Kafka UI      | http://localhost:8090   |
| Kafka (host)  | localhost:29092         |

---

## 📝 Sending Logs

The request shape is identical regardless of which ingestion transport is active -
the transport switch is invisible to the API caller.

```bash
curl -X POST http://localhost:8080/logs \
  -H "Content-Type: application/json" \
  -d '{"timestamp":"2025-11-22T09:00:00","level":"INFO","message":"Hello from Docker","service":"app"}'
```

Batch endpoint:

```bash
curl -X POST http://localhost:8080/logs/batch \
  -H "Content-Type: application/json" \
  -d '[{"timestamp":"2025-11-22T09:00:00","level":"INFO","message":"one","service":"app"},
       {"timestamp":"2025-11-22T09:00:01","level":"WARN","message":"two","service":"app"}]'
```

---

## 📡 Kafka Ingestion

### Switching modes

Controlled by one property, `pipeline.ingestion.mode`, settable via the
`PIPELINE_INGESTION_MODE` environment variable:

| Mode | Behavior |
|---|---|
| `direct` (default) | Original path. No Kafka broker required at all. |
| `kafka` | Logs are published to Kafka; a consumer drains the topic into `BatchWriter`. |

`docker-compose.yml` ships with `PIPELINE_INGESTION_MODE: kafka` for `log-service`
so the full stack demonstrates Kafka out of the box. To run without Kafka, either
change that value to `direct` in `docker-compose.yml`, or override it:

```bash
PIPELINE_INGESTION_MODE=direct docker compose up --build log-service
```

### Watching it happen

Open **http://localhost:8090** (Kafka UI):

1. **Topics → logs** - see the message land in a partition. Messages are keyed by
   `service`, so all logs from the same service always land on the same partition
   (this keeps their order, and spreads load across partitions).
2. **Consumers → log-pipeline** - watch the consumer group's lag return to 0 as
   `LogConsumer` reads and hands each message to `BatchWriter`.
3. `log-service` container logs will show `Wrote N logs to ClickHouse` from
   `BatchWriter`, exactly as in direct mode.

### How the producer works

`LogProducer` (`kafka/LogProducer.java`) is only active in `kafka` mode. It
serializes `LogDTO` to JSON (reusing Spring Boot's `ObjectMapper`, so
`LocalDateTime` fields serialize correctly) and publishes it to the `logs` topic,
keyed by `service`. Sending is asynchronous - `IngestionService.ingest()` returns
immediately, matching the project's "high throughput over low latency" priority.

### How the consumer works

`LogConsumer` (`kafka/LogConsumer.java`) is a `@KafkaListener` on the `logs` topic
running with concurrency 3 (one thread per partition). For each message it calls
`BatchWriter.enqueue()` - nothing else. If that queue happens to be full, the log
is dropped with a warning, the same behavior direct-mode ingestion already has.
Offsets commit automatically after each message is handed off successfully; a
message that fails to deserialize (e.g. malformed JSON) is logged and skipped by
an `ErrorHandlingDeserializer` instead of stalling the consumer on that offset
forever.

### Troubleshooting

- **`log-service` won't start / connection refused to Kafka** - the `kafka`
  service has a healthcheck, and `log-service` waits for it
  (`depends_on: kafka: condition: service_healthy`). Give the broker ~20-30s on
  first boot.
- **No messages showing in Kafka UI** - confirm `PIPELINE_INGESTION_MODE=kafka`
  is actually set on `log-service` (`docker compose exec log-service env | grep PIPELINE`).
- **Want to inspect Kafka from the host** instead of Kafka UI - it's mapped to
  `localhost:29092`, e.g. with `kcat -b localhost:29092 -L`.

---

## 📊 Grafana Setup

### Screenshot of Grafana Dashboard

![Grafana Dashboard](./Grafana%20Dashboard.png)

### Add ClickHouse Data Source

URL: http://clickhouse:8123

Create panels with queries like:

```sql
SELECT timestamp, level, message
FROM logs
ORDER BY timestamp DESC
```

---

## ✅ Running the Tests

```bash
./mvnw test
```

**No Docker, no ClickHouse, and no external Kafka broker are required.** Every Spring
test replaces `JdbcTemplate` with a Mockito mock, so nothing ever opens a real
database connection; the one test that needs a Kafka broker
(`KafkaModeIngestionIntegrationTest`) uses `@EmbeddedKafka`, an in-JVM broker that
starts and stops with the test itself. The whole suite - 44 tests - runs in well
under 15 seconds.

| Layer | Classes | What it proves |
|---|---|---|
| Unit | `BatchWriterTest`, `IngestionServiceTest`, `LogProducerTest`, `LogConsumerTest`, `LogDtoSerializationTest` | Batching, flushing, overflow handling, concurrent producers, mode routing, and JSON (de)serialization - all with mocked collaborators, no Spring context. |
| Web slice | `LogControllerTest` | `@WebMvcTest` + MockMvc: request validation and delegation, with `IngestionService` mocked. |
| Integration | `ClickHouseLogPipelineApplicationTests`, `DirectModeIngestionIntegrationTest`, `KafkaModeIngestionIntegrationTest` | Full Spring context for each mode, proving `@ConditionalOnProperty` really does keep Kafka out of direct mode, and that a request in Kafka mode flows through a real producer → broker → consumer chain into `BatchWriter`. |

Run a single class or a `@Nested` group directly:

```bash
./mvnw test -Dtest=BatchWriterTest
./mvnw test -Dtest=BatchWriterTest\$Concurrency
```

`./mvnw clean package` runs the full suite as part of the build (tests are **not**
skipped by default).
