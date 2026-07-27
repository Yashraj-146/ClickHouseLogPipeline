# CLAUDE.md

# ClickHouse Log Pipeline
*A high-throughput log ingestion system built with Spring Boot, ClickHouse, Grafana and Docker.*

---

# Purpose

This project implements a lightweight log ingestion pipeline intended to demonstrate the architecture of a production-style logging service.

The design prioritizes:

- high ingestion throughput
- asynchronous processing
- batch inserts into ClickHouse
- low latency REST API
- clean separation of responsibilities
- easy extensibility

Kafka was intentionally deferred at the start of this project so the core batching architecture could be understood on its own first. It has since been added as an additional ingestion transport - see below. Redis and other infrastructure remain deferred.

---

# High Level Architecture

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

The REST API **never writes directly to ClickHouse.**

Instead it only validates the request and either enqueues it directly or publishes it to Kafka.

The BatchWriter owns all database writes, regardless of which transport the log arrived through.

Kafka is a transport layer only. It never writes to ClickHouse - the Kafka consumer calls `BatchWriter.enqueue()`, the same as the direct path.

---

# Project Structure

```
src/main/java/com/yashraj/clickhousepipeline

│
├── ClickHouseLogPipelineApplication.java
│
├── controller
│      └── LogController.java
│
├── dto
│      └── LogDTO.java
│
├── service
│      ├── IngestionService.java
│      └── BatchWriter.java
│
├── kafka
│      ├── LogProducer.java
│      └── LogConsumer.java
│
└── config
       ├── ClickHouseConfig.java
       ├── KafkaTopicConfig.java
       ├── KafkaProducerConfig.java
       └── KafkaConsumerConfig.java
```

Resources

```
src/main/resources/

application.properties
```

Infrastructure

```
Dockerfile
docker-compose.yml
pom.xml
```

---

# Execution Flow

## 1.

Client sends

POST /logs

Example

```json
{
    "timestamp":"2025-11-22T09:00:00",
    "level":"INFO",
    "message":"Application Started",
    "service":"backend"
}
```

---

## 2.

LogController

Responsibilities

- validates request
- delegates only
- contains no business logic

Calls

```
IngestionService.ingest(...)
```

---

## 3.

IngestionService

Responsibilities

- very thin service layer
- reads `pipeline.ingestion.mode` (direct or kafka)
- direct mode: sends LogDTO straight to BatchWriter
- kafka mode: sends LogDTO to LogProducer instead

No persistence logic exists here. No Kafka client code exists here either -
publishing itself is delegated to LogProducer.

---

## 3a. (kafka mode only)

LogProducer

Responsibilities

- publishes LogDTO as JSON to the "logs" Kafka topic
- uses `service` as the partition key so a given service's logs stay ordered

---

## 3b. (kafka mode only)

Kafka Topic "logs"

- 3 partitions, replication factor 1 (single-broker dev cluster)
- created at startup via a `KafkaAdmin.NewTopics` bean, not manually

---

## 3c. (kafka mode only)

LogConsumer

Responsibilities

- `@KafkaListener` on the "logs" topic, concurrency 3
- calls `BatchWriter.enqueue()` - identical to the direct-mode path from here on
- never writes to ClickHouse itself

If a message fails to deserialize (malformed JSON), an `ErrorHandlingDeserializer`
logs it and moves on instead of retrying the same offset forever. This is not a
dead-letter queue - the message is simply dropped after logging.

---

## 4.

BatchWriter

This is the heart of the system.

Responsibilities

- owns BlockingQueue
- owns scheduler
- owns writer thread pool
- performs batch inserts
- handles queue overflow
- isolates ClickHouse interaction

Nothing else in the application writes to ClickHouse.

---

## 5.

Blocking Queue

Current implementation

```
LinkedBlockingQueue<LogDTO>
```

Capacity

```
100000
```

Reasons

- decouples API latency from database latency
- prevents controller from blocking
- absorbs spikes

---

## 6.

Flush Scheduler

Runs every

```
1000 ms
```

or whenever queue reaches

```
1000 logs
```

This provides

- reduced network overhead
- fewer ClickHouse insert operations
- higher throughput

---

## 7.

ClickHouse

Insertion performed using

```
JdbcTemplate.batchUpdate()
```

Current SQL

```sql
INSERT INTO logs
(timestamp, level, message, service)
VALUES (?, ?, ?, ?)
```

---

# Design Decisions

## Controller should remain thin

Never place batching logic inside controllers.

---

## Service layer remains minimal

Business logic should stay inside BatchWriter.

---

## Single Writer Component

All writes should go through BatchWriter.

Avoid creating multiple independent ClickHouse writers.

This still holds with Kafka in the picture. LogConsumer is not a writer - it only
calls BatchWriter.enqueue(), the same entry point the direct path uses.

---

## Kafka is transport only

Kafka does not replace the BlockingQueue / BatchWriter batching mechanism.

It sits in front of it, as an alternative way for a log to reach
`BatchWriter.enqueue()`.

```
Direct mode:  Controller -> Service -> BatchWriter -> Queue -> Database

Kafka mode:   Controller -> Service -> Producer -> Topic -> Consumer -> BatchWriter -> Queue -> Database
```

Both modes converge on the same queue, the same scheduler, and the same
`JdbcTemplate.batchUpdate()` call. Nothing about batching or database writes
changes based on which transport was used.

The mode is selected with `pipeline.ingestion.mode` (`direct` or `kafka`). In
direct mode, none of the Kafka beans are even created
(`@ConditionalOnProperty`), so the app has no dependency on a running broker.

---

## Queue Based Architecture

Current architecture intentionally uses

```
Controller

↓

Queue

↓

Batch Writer

↓

Database
```

instead of

```
Controller

↓

Database
```

This is the main architectural decision of the project.

---

## DTO

LogDTO represents the ingestion payload.

Avoid exposing database entities directly.

---

# Current Docker Architecture

```
Docker Compose

│

├── clickhouse

├── grafana

├── kafka

├── kafka-ui

└── log-service
```

Container communication

```
Spring Boot

↓

clickhouse:8123

↓ (kafka mode only)

kafka:9092
```

No localhost communication inside containers.

kafka-ui (http://localhost:8090) is a browser UI for inspecting topics, messages,
partitions, and consumer lag. It is not on the write path.

---

# Configuration

application.properties contains

- datasource
- driver
- logging
- pipeline.ingestion.mode (direct or kafka)
- pipeline.kafka.topic, pipeline.kafka.partitions, pipeline.kafka.consumer.concurrency
- spring.kafka.bootstrap-servers, spring.kafka.consumer.group-id

Docker overrides datasource URL and Kafka settings using environment variables.

---

# Technologies

Java 17

Spring Boot 3

ClickHouse JDBC

Spring JDBC

Spring Kafka

Apache Kafka (KRaft mode, no ZooKeeper)

Grafana

Docker

Docker Compose

Maven

---

# Important Notes

Spring Session JDBC was intentionally removed.

Do NOT reintroduce it.

ClickHouse has no official Spring Session schema.

---

Compression is disabled

Current datasource

```
jdbc:clickhouse://clickhouse:8123/logs?compress=0
```

Reason

The default LZ4 configuration caused runtime failures.

---

# Current Limitations

No retry mechanism.

No dead-letter queue.

No metrics endpoint.

No authentication.

No rate limiting.

No health checks.

No graceful shutdown persistence.

No backpressure. This applies to Kafka mode too - LogConsumer has no mechanism to
pause consumption when BatchWriter's queue is full; it drops and logs a warning,
same as direct mode.

No Kafka dead-letter queue. A malformed Kafka message is logged and skipped, not
routed anywhere for reprocessing.

These are intentional and reserved for future improvements.

# Files Claude Should Read First

Only these files are needed to understand the project.

```
1.
BatchWriter.java

2.
IngestionService.java

3.
LogController.java

4.
ClickHouseConfig.java

5.
LogDTO.java

6.
application.properties

7.
docker-compose.yml

8.
Dockerfile

9.
pom.xml

10.
LogProducer.java

11.
LogConsumer.java
```

Everything else is secondary.

---

# Extension Guidelines

When implementing new functionality

Prefer

```
Controller

↓

Service

↓

BatchWriter

↓

Database
```

Do not bypass BatchWriter.

---

If additional metadata is required

Example

- hostname
- request id
- environment
- region

Extend

```
LogDTO
```

rather than introducing another DTO.

---

If introducing new storage

Create a new Writer component.

Do not overload BatchWriter with unrelated persistence logic.

---

# Performance Philosophy

Current priority

High Throughput
>

Low Latency

The API should return quickly.

Persistence happens asynchronously.

---

# Goal

Maintain a clean, understandable codebase that demonstrates a production-inspired ingestion architecture while remaining simple enough for interviews, portfolio demonstrations, and future feature additions.