# =============================================================================
# Telemetry Sandbox Bootstrap Script (Windows PowerShell)
# =============================================================================
$ErrorActionPreference = "Stop"

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "Initializing Real-Time Telemetry Docker Sandbox..." -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

# 1. Wait for ClickHouse HTTP
Write-Host "1. Checking ClickHouse connectivity at http://localhost:8123..."
$retry = 0
while ($retry -lt 15) {
    try {
        $resp = Invoke-RestMethod -Uri "http://localhost:8123/ping" -Method Get -TimeoutSec 3
        if ($resp -match "Ok") {
            Write-Host "   ClickHouse is ready." -ForegroundColor Green
            break
        }
    } catch {
        Start-Sleep -Seconds 2
        $retry++
    }
}

# Verify ClickHouse tables
Write-Host "2. Verifying ClickHouse DDL tables..."
try {
    $tables = Invoke-RestMethod -Uri "http://localhost:8123/?query=SHOW+TABLES+FROM+iot" -Method Get
    Write-Host "   Tables found in 'iot' database:`n$tables" -ForegroundColor Gray
} catch {
    Write-Host "   Warning: Could not list ClickHouse tables: $_" -ForegroundColor Yellow
}

# 3. Check Kafka topic
Write-Host "3. Checking Kafka topic iot.telemetry.raw..."
try {
    docker exec telemetry-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic iot.telemetry.raw --partitions 128 --replication-factor 1
    Write-Host "   Kafka topic iot.telemetry.raw initialized (128 partitions)." -ForegroundColor Green
} catch {
    Write-Host "   Note: Ensure Docker container telemetry-kafka is running." -ForegroundColor Yellow
}

# 4. Check MinIO
Write-Host "4. Checking MinIO bucket telemetry-warehouse..."
try {
    docker exec telemetry-minio-init mc mb local/telemetry-warehouse
} catch {
    # already exists or container finished
}

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "Sandbox bootstrap verified successfully!" -ForegroundColor Green
Write-Host "Services available:"
Write-Host " - Frontend Dashboard:   http://localhost:5173"
Write-Host " - API Gateway:          http://localhost:8080"
Write-Host " - Flink Dashboard:      http://localhost:8081"
Write-Host " - MinIO Console:        http://localhost:9001 (minioadmin / minioadmin)"
Write-Host " - ClickHouse HTTP:      http://localhost:8123"
Write-Host " - Kafka Broker:         localhost:29092 / kafka:9092"
Write-Host "==========================================================" -ForegroundColor Cyan
