#!/usr/bin/env bash
# =============================================================================
# Telemetry Sandbox Bootstrap Script (Linux / macOS)
# =============================================================================
set -euo pipefail

echo "=========================================================="
echo "Initializing Real-Time Telemetry Docker Sandbox..."
echo "=========================================================="

# 1. Wait for ClickHouse HTTP
echo "1. Checking ClickHouse connectivity at http://localhost:8123..."
until curl -s "http://localhost:8123/ping" | grep -q "Ok"; do
  echo "   Waiting for ClickHouse..."
  sleep 2
done
echo "   ClickHouse is ready."

# Verify ClickHouse tables
echo "2. Verifying ClickHouse DDL tables..."
curl -s "http://localhost:8123/?query=SHOW+TABLES+FROM+iot"
echo ""

# 3. Check Kafka topic
echo "3. Checking Kafka topic iot.telemetry.raw..."
if docker exec telemetry-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list | grep -q "iot.telemetry.raw"; then
  echo "   Topic iot.telemetry.raw exists."
else
  echo "   Creating topic iot.telemetry.raw with 128 partitions..."
  docker exec telemetry-kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --create --if-not-exists \
    --topic iot.telemetry.raw \
    --partitions 128 \
    --replication-factor 1
fi

# 4. Check MinIO bucket
echo "4. Checking MinIO bucket telemetry-warehouse..."
docker exec telemetry-minio mc ready local || true

echo "=========================================================="
echo "Sandbox bootstrap verified successfully!"
echo "Services available:"
echo " - Frontend Dashboard:   http://localhost:5173"
echo " - API Gateway:          http://localhost:8080"
echo " - Flink Dashboard:      http://localhost:8081"
echo " - MinIO Console:        http://localhost:9001 (minioadmin / minioadmin)"
echo " - ClickHouse HTTP:      http://localhost:8123"
echo " - Kafka Broker:         localhost:29092 / kafka:9092"
echo "=========================================================="
