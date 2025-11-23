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

---

## 📝 Test Log Ingestion

Send a POST request:

```bash
curl -X POST http://localhost:8080/logs \
  -H "Content-Type: application/json" \
  -d '{"timestamp":"2025-11-22T09:00:00","level":"INFO","message":"Hello from Docker","service":"app"}'
```

## 📊 Grafana Setup

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
│       │   ├── controller/
│       │   ├── dto/
│       │   └── service/
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
