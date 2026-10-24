## Problem Statement

Plant operations and telemetry engineers need to monitor over 5,000,000 industrial IoT sensors generating measurements every second ($5,000,000\text{ rows/sec}$). In high-density monitoring centers, operators must inspect long-term trends spanning up to 30 days across multiple sensors simultaneously, while also smoothly zooming in to individual 1-second measurements and observing live incoming sensor telemetry at 60 fps.

Existing time-series systems either crash the web browser due to multi-megabyte JSON payloads and garbage collection stalls, or impose prohibitive infrastructure and commercial licensing costs when scaling out to 35+ TB of monthly historical telemetry. Operators require an ultra-fast, zero-stutter visualization pipeline that retains critical spikes and transients without loss, backed entirely by cost-effective open-source infrastructure.

## Solution

A hybrid tiered lakehouse and dual-serving architecture combining Apache Kafka, Apache Flink, Apache Iceberg, an open-source REST Catalog, ClickHouse OLAP, a Spring Boot reactive gateway, and a Vue 3 WebGL frontend:

1. **Stream Processing & Lakehouse Ingestion**: Apache Kafka receives 1-second batched sensor telemetry across 128 partitions. Apache Flink serves as the sole consumer, streaming sorted 256MB ZSTD Parquet files into Apache Iceberg on internal S3 using composite `bucket(32, sensor_id), hours(ts)` partitioning, while simultaneously pre-computing 1-minute and 1-hour AM4 (Aggregation-based M4) downsampled rollups.
2. **Metadata & Snapshot Speed**: An open-source Iceberg REST Catalog (Apache Polaris / Project Nessie) tracks table snapshots, allowing instant $O(1)$ metadata resolution without costly S3 directory crawls.
3. **High-Performance OLAP Serving**: ClickHouse stores the 1-minute and 1-hour AM4 rollups as standard 8-scalar column tables in `MergeTree` engines, and queries Iceberg Parquet files via the REST Catalog with local NVMe `filesystem_cache` acceleration.
4. **Dual-Serving Gateway**: Spring Boot 3.4+ provides a unified query router. For historical telemetry ($\Delta T > 60\text{s}$), it streams ClickHouse `FORMAT ArrowStream` binary payloads directly to the client via Zero-Copy HTTP pass-through. For live telemetry ($0 \le \Delta T \le 60\text{s}$), it filters Kafka events and pushes 1-second records directly to active client dashboards over WebSocket.
5. **Zero-Copy WebGL Visualization**: Vue 3 decodes Apache Arrow IPC binary buffers directly into native typed arrays using `@uwdata/flechette`, verifies stream integrity, and injects data directly into LightningChart JS WebGL buffers, combining historical trends with real-time `appendSamples()` live tailing.

## User Stories

1. As an operations engineer, I want to load a 1-month trend for 10 selected sensors within 100 milliseconds, so that I can immediately assess operational stability over long time windows.
2. As an operations engineer, I want to zoom from a 30-day macro view down to a 1-second micro view without page reloads, so that I can pinpoint the exact root cause of transient anomalies.
3. As a monitoring operator, I want to see live sensor telemetry streaming continuously onto the active chart at 60 fps, so that I can observe machine state changes with zero perceived lag.
4. As an operations engineer, I want the chart downsampling to strictly preserve local minima, maxima, first, and last points (AM4), so that critical voltage or pressure spikes are never smoothed away.
5. As a frontend user, I want panning across large time-series datasets to maintain 60 fps without garbage collection frame drops, so that my dashboard interaction remains fluid and responsive.
6. As an infrastructure administrator, I want to store monthly 35TB raw Parquet datasets on internal S3-compatible storage rather than high-density NVMe arrays, so that storage TCO is minimized.
7. As an enterprise architect, I want the backend storage, stream processing, and database engines to be 100% open-source without commercial licenses, so that capacity can be scaled without licensing hurdles.
8. As a backend developer, I want Spring Boot to stream Arrow IPC bytes directly from ClickHouse to HTTP responses with zero JVM heap buffer copying, so that the API layer handles high concurrent query loads with minimal memory.
9. As a frontend developer, I want the client parser to validate the Arrow IPC end-of-stream marker and recover gracefully from interrupted streams, so that network disruptions do not freeze the UI.
10. As a data engineer, I want Flink to write Parquet files using `bucket(32, sensor_id)` hash distribution, so that small fragmented files are eliminated and target file sizes remain around 256MB.
11. As a database administrator, I want ClickHouse to store 1-minute and 1-hour rollups as standard 8-scalar columns rather than proprietary binary aggregate states, so that table schemas remain robust across version upgrades.
12. As a system operator, I want repeated queries and neighboring zoom ranges to be served from local ClickHouse NVMe filesystem cache, so that repetitive S3 GET operations are eliminated.
13. As an operations engineer, I want to inspect and compare up to 50 distinct sensor series on a single synchronized viewport, so that cross-sensor dependencies can be visually correlated.
14. As an operator, I want an integrated mini-map (ZoomBandChart) displaying the entire 30-day outline at the bottom of the screen, so that I maintain global context while inspecting zoomed-in details.
15. As a network administrator, I want telemetry endpoints to operate with HTTP compression disabled and proxy buffering disabled, so that time-to-first-byte (TTFB) and streaming latency are minimized.

## Implementation Decisions

- **Kafka Ingestion Topology**: 128 partitions on a dedicated cluster receiving batched Protobuf payloads from edge gateways.
- **Unified Stream Processing with Flink**: Flink 1.20+ is the single consumer of raw telemetry from Kafka. It handles both Iceberg Parquet sinking with hash distribution and tumbling-window AM4 pre-aggregation for 1-minute and 1-hour intervals.
- **Scalar 8-Column Rollup Schema**: Rather than storing proprietary ClickHouse `AggregateFunction` binary blobs, Flink writes 8 standard scalar fields (`first_ts, first_val, min_val_ts, min_val, max_val_ts, max_val, last_ts, last_val`) into ClickHouse `MergeTree` tables, ensuring portability and schema evolution safety.
- **OSS Iceberg REST Catalog**: Integration of an open-source REST Catalog (Apache Polaris / Project Nessie) to serve table snapshots to ClickHouse and Flink in $O(1)$ time, bypassing S3 object listing latency.
- **ClickHouse Query Gateway with NVMe Cache**: ClickHouse 24.8 LTS+ acts as the single query execution engine for historical data. S3 Parquet reads utilize `filesystem_cache` on local U.3 NVMe drives to guarantee 50~180ms latency for detailed zoom ranges.
- **Spring Boot Dual-Serving Architecture**:
  - Historical query path: Dynamic routing based on requested interval $\Delta T$ and pixel width $W$, streaming `FORMAT ArrowStream` via `StreamingResponseBody` with zero intermediate DTO allocation.
  - Real-time live path: Spring Boot filters Kafka records for active sensor subscriptions and pushes 1-second records over WebSocket.
- **Vue 3 Zero-Copy WebGL Pipeline**:
  - `@uwdata/flechette` decodes incoming Arrow IPC streams into `Uint32Array` and `Float64Array` memory views.
  - Sliced TypedArrays are bound directly to LightningChart JS `PointLineAreaSeries.appendSamples()`.
  - Arrow EOS marker (0xFFFFFFFF) detection with exponential backoff retry on mid-stream failure.
  - Programmatic zoom updates guarded by `isProgrammaticUpdate` flag and `AbortController` to prevent circular re-query loops.

## Testing Decisions

- **Test Quality Definition**: Tests must verify external observable behavior (HTTP response formats, binary stream decodability, WebSocket push events, Parquet schema compliance) rather than internal implementation details or private methods.
- **Spring Boot Gateway Seam**:
  - Test via Testcontainers running ClickHouse and Kafka.
  - Verify routing correctness across $\Delta T > 7\text{d}$, $6\text{h} < \Delta T \le 7\text{d}$, $60\text{s} < \Delta T \le 6\text{h}$, and live WebSocket subscriptions.
  - Assert HTTP responses stream valid Apache Arrow IPC formats with expected column headers and zero JVM heap expansion.
- **Flink MiniCluster Seam**:
  - Test via Flink `MiniClusterWithClientResource`.
  - Ingest synthetic Protobuf records and assert Iceberg Parquet generation under `bucket(32, sensor_id)` partitioning.
  - Verify mathematical accuracy of the 8 scalar AM4 extrema in ClickHouse sink records across 1-minute and 1-hour tumbling windows.
- **Frontend Composable Seam**:
  - Vitest / Playwright component tests feeding mock Arrow IPC ArrayBuffers and WebSocket payloads into `useTelemetryStream`.
  - Verify seamless decoding via Flechette, WebGL buffer ingestion, and resilient recovery when simulated network streams are terminated prematurely.

## Out of Scope

- Public cloud provider S3 services (AWS S3, Google Cloud Storage); the system is architected exclusively for on-premise S3 (RustFS / Ceph RGW).
- Enterprise commercial time-series database licenses or paid cloud tiering services.
- Machine learning anomaly prediction or automated root cause inference engines.
- Client-side persistent caching beyond the active WebGL buffer memory.

## Further Notes

- Governed by ADR-001 (Accepted).
- Built to sustain an ingestion rate of 5,000,000 rows/second and an active query volume of multi-sensor 30-day analytics.
