-- ADR-001: 500만 IoT 센서 시계열 데이터 스토리지 초기 DDL 명세
-- 엔진: ClickHouse 24.8 LTS+

CREATE DATABASE IF NOT EXISTS iot;

-- 1. 1분 AM4 스칼라 사전 집계 테이블 (Intermediate Range: 6h < ΔT <= 7d)
CREATE TABLE IF NOT EXISTS iot.sensor_rollup_1m (
    sensor_id   UInt32,
    bucket_ts   DateTime('UTC'),
    first_ts    DateTime64(3, 'UTC'),
    first_val   Float64,
    min_val_ts  DateTime64(3, 'UTC'),
    min_val     Float64,
    max_val_ts  DateTime64(3, 'UTC'),
    max_val     Float64,
    last_ts     DateTime64(3, 'UTC'),
    last_val    Float64
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(bucket_ts)
ORDER BY (sensor_id, bucket_ts)
SETTINGS index_granularity = 8192;

-- 2. 1시간 AM4 스칼라 사전 집계 테이블 (Macro 30-Day Range: ΔT > 7d)
CREATE TABLE IF NOT EXISTS iot.sensor_rollup_1h (
    sensor_id   UInt32,
    bucket_ts   DateTime('UTC'),
    first_ts    DateTime64(3, 'UTC'),
    first_val   Float64,
    min_val_ts  DateTime64(3, 'UTC'),
    min_val     Float64,
    max_val_ts  DateTime64(3, 'UTC'),
    max_val     Float64,
    last_ts     DateTime64(3, 'UTC'),
    last_val    Float64
) ENGINE = MergeTree()
PARTITION BY toYYYY(bucket_ts)
ORDER BY (sensor_id, bucket_ts)
SETTINGS index_granularity = 8192;

-- 3. Iceberg REST Catalog 연동 원본 시계열 테이블 (Micro-Zoom Range: ΔT <= 6h)
-- ClickHouse Iceberg 엔진을 통해 S3 Parquet 바이트레인지 스캔 및 로컬 NVMe filesystem_cache 활용
CREATE TABLE IF NOT EXISTS iot.sensor_iceberg_raw (
    sensor_id   UInt32,
    ts_ms       Int64,
    val         Float64
) ENGINE = MergeTree()
ORDER BY (sensor_id, ts_ms);
