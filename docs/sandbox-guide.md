# Docker Sandbox Environment & Smoke Verification Guide

This guide describes how to run and verify the full-stack real-time telemetry service locally using Docker Compose, bootstrapping all infrastructure services with zero external cloud dependencies.

---

## 1. Architecture & Port Mapping

The local sandbox orchestrates 6 primary services and 2 initialization helpers:

| Container Name | Service | Internal Port | Host Port | Purpose |
| :--- | :--- | :--- | :--- | :--- |
| `telemetry-kafka` | Apache Kafka 3.8 (KRaft) | `9092`, `9093` | `9092`, `29092` | Telemetry event log (`iot.telemetry.raw`) |
| `telemetry-kafka-init` | Topic Initializer | - | - | Auto-creates `iot.telemetry.raw` (128 partitions) |
| `telemetry-clickhouse` | ClickHouse Server 24.8 LTS | `8123`, `9000` | `8123`, `9000` | Analytics rollups (`1m`, `1h`) & Arrow queries |
| `telemetry-minio` | MinIO Object Storage | `9000`, `9001` | `9000`, `9001` | S3-compatible Iceberg warehouse (`telemetry-warehouse`) |
| `telemetry-minio-init` | Bucket Initializer | - | - | Auto-creates `telemetry-warehouse` bucket |
| `telemetry-flink-jobmanager` | Apache Flink 1.20 | `8081` | `8081` | Flink Web UI & Master coordinator |
| `telemetry-flink-taskmanager` | Apache Flink 1.20 | - | - | Flink Stream workers (8 slots) |
| `telemetry-gateway` | Spring Boot 3.4 Gateway | `8080` | `8080` | Direct Arrow stream & WebSocket live bridge |
| `telemetry-frontend` | Vite Vue 3 Dashboard | `80` | `5173` | WebGL telemetry charting UI with Nginx proxy |

---

## 2. Prerequisites & Building Artifacts

Before starting the containers, package the Spring Boot Gateway and Vite Frontend:

```bash
# 1. Package Spring Boot Gateway JAR
cd backend
gradle :gateway:bootJar
cd ..

# 2. Package Vite Frontend distribution
cd frontend
npm run build
cd ..
```

---

## 3. Starting the Sandbox

Start all infrastructure and application services in detached mode:

```bash
docker compose up -d --build
```

Monitor container health and startup logs:

```bash
docker compose ps
docker compose logs -f
```

---

## 4. Bootstrapping & Health Verification

Run the bootstrap initialization script to confirm all database tables, Kafka topics, and S3 buckets are ready:

### Linux / macOS
```bash
chmod +x scripts/init-sandbox.sh
./scripts/init-sandbox.sh
```

### Windows PowerShell
```powershell
.\scripts\init-sandbox.ps1
```

---

## 5. Automated End-to-End Smoke Verification

Run the automated smoke test script to verify:
1. ClickHouse HTTP connectivity and table existence.
2. Spring Boot Gateway HTTP binary Arrow streaming (`/api/v1/telemetry/am4-arrow`).
3. Frontend accessibility via Nginx reverse proxy.

### Linux / macOS
```bash
chmod +x scripts/e2e-smoke-test.sh
./scripts/e2e-smoke-test.sh
```

### Windows PowerShell
```powershell
.\scripts\e2e-smoke-test.ps1
```

---

## 6. Accessing Local Dashboards

- **Frontend Telemetry Dashboard**: [http://localhost:5173](http://localhost:5173)
- **API Gateway Health & Endpoints**: [http://localhost:8080](http://localhost:8080)
- **Flink Dashboard**: [http://localhost:8081](http://localhost:8081)
- **MinIO Console**: [http://localhost:9001](http://localhost:9001) (User: `minioadmin`, Password: `minioadmin`)
- **ClickHouse HTTP**: [http://localhost:8123](http://localhost:8123)

---

## 7. Teardown

To stop and remove all sandbox containers and volumes:

```bash
docker compose down -v
```
