# ClickHouse Log Pipeline (Spring Boot + Docker + Grafana)

This project is a lightweight, production-style log ingestion pipeline using:

- **Spring Boot** – REST API to ingest logs
- **ClickHouse** – High-performance OLAP database
- **Grafana** – Dashboards for visualization
- **Docker Compose** – fully containerized environment

---

## 🚀 Features

- Accept logs via REST endpoint `/logs`
- Async queue + batch writer for ultra-fast ingestion
- Optional Kafka transport in front of the same queue (see below)
- ClickHouse table for log storage
- Grafana dashboards (real-time log monitoring)
- Fully dockerized system

---

## 🐳 Run via Docker

### 1. Build the app:
mvn clean package -DskipTests


### 2. Start the full stack:
docker compose up --build


## Services:

| Service       | URL |
|---------------|-------------------------|
| Spring Boot   | http://localhost:8080   |
| Grafana       | http://localhost:3000   |
| ClickHouse UI | http://localhost:8123   |
| Kafka UI      | http://localhost:8090   |
| Kafka (host)  | localhost:29092         |

---

## 📡 Kafka Ingestion (optional transport)

This is the project's first use of Kafka, so this section spells out everything
needed to run it - no prior Kafka experience assumed.

### What it is

The REST API and the batching mechanism (`BlockingQueue` → scheduled flush →
`BatchWriter` → ClickHouse) are unchanged. Kafka is only an **additional way for a
log to reach that same queue**:

```
Direct mode:  Controller -> Service -> BatchWriter.enqueue() -> Queue -> ClickHouse

Kafka mode:   Controller -> Service -> Producer -> Kafka topic "logs" -> Consumer
              -> BatchWriter.enqueue() -> Queue -> ClickHouse
```

The Kafka consumer never touches ClickHouse directly - it calls the exact same
`BatchWriter.enqueue()` method the direct path uses.

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

### Running it

```bash
docker compose up --build
```

This starts a single-node Kafka broker in **KRaft mode** (`apache/kafka:3.9.1` -
no ZooKeeper needed) and **Kafka UI** (`kafbat/kafka-ui:v1.5.0`) alongside
ClickHouse, Grafana, and the app. The "logs" topic (3 partitions, replication
factor 1) is created automatically at application startup - no manual topic
creation step.

### Sending a log through Kafka

Once `log-service` is up in `kafka` mode, the request is identical to direct mode
- the transport switch is invisible to the API caller:

```bash
curl -X POST http://localhost:8080/logs \
  -H "Content-Type: application/json" \
  -d '{"timestamp":"2025-11-22T09:00:00","level":"INFO","message":"Hello via Kafka","service":"app"}'
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

## 📝 Test Log Ingestion

Send a POST request:

```bash
curl -X POST http://localhost:8080/logs \
  -H "Content-Type: application/json" \
  -d '{"timestamp":"2025-11-22T09:00:00","level":"INFO","message":"Hello from Docker","service":"app"}'
```

## 📊 Grafana Setup
## Screenshot of Grafana Dashboard
![Grafana Dashboard](./Grafana%20Dashboard.png)

## Add ClickHouse Data Source

URL: http://clickhouse:8123

Create panels with queries like:

```sql
SELECT timestamp, level, message
FROM logs
ORDER BY timestamp DESC
```

## ⚙️ Tech Stack

- Java 17
- Spring Boot 3
- ClickHouse
- Grafana
- Docker + Docker Compose

## 📦 Project Structure
```css
clickhouselogpipeline/
│
├── src/
│   └── main/
│       ├── java/com/yashraj/clickhousepipeline/
│       │   ├── ClickHouseLogPipelineApplication.java
│       │   ├── controller/LogController.java
│       │   ├── dto/LogDTO.java
│       │   ├── service/BatchWriter.java
│       │   ├── service/IngestionService.java
│       │   ├── kafka/LogProducer.java
│       │   ├── kafka/LogConsumer.java
│       │   ├── config/ClickHouseConfig.java
│       │   ├── config/KafkaTopicConfig.java
│       │   ├── config/KafkaProducerConfig.java
│       │   └── config/KafkaConsumerConfig.java
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
├── .gitignore
└── HELP.md (optional)
```

---

# ✅ 4. Create Git Repository

In terminal:
```bash
cd /Users/yashraj146/Documents/clickhouselogpipeline

git init
git add .
git commit -m "Initial commit: Dockerized ClickHouse log pipeline"
```
