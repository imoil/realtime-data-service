## Summary

Comprehensive architectural refactoring of the real-time telemetry service based on the post-implementation retrospective. Applying the **codebase-design deep module** principles, this specification deepens the backend query seam, unifies the Flink stream processing topology to eliminate duplicate deserialization/watermarks, decouples the frontend visualization layer through a headless dashboard coordinator (`useTelemetryDashboard`), and implements client-side fixed-budget ring buffers for uninterrupted 24/7 monitoring.

1. **Unified Telemetry Storage Engine (`TelemetryStorageEngine`)**: Consolidates `TelemetryQueryRouter` and `ClickHouseTelemetryService` behind a single deep interface (`fetchTelemetry(TelemetryQueryRequest): TelemetryStreamResult`), hiding ClickHouse SQL generation, multi-tier routing (Iceberg raw vs. 1m vs. 1h rollups), single-scan AM4 downsampling, and metadata extraction.
2. **Unified Stream Topology (`TelemetryPipelineTopology`)**: Restructures Flink stream execution to perform Protobuf deserialization and event-time watermark assignment exactly once, branching into three dedicated sink streams (Iceberg Raw Sink `bucket(32)`, 1-minute Rollup MergeTree Sink, and 1-hour Rollup MergeTree Sink).
3. **Deep Telemetry Dashboard Coordinator (`useTelemetryDashboard`)**: Extracts all state orchestration, HTTP stream fetching, WebSocket subscription lifecycle, 120ms debounce logic, and programmatic loop guards from `TelemetryChart.vue` into a headless deep composable.
4. **Fixed-Budget WebGL Ring Buffer Pooling**: Implements circular buffer pooling for live samples to guarantee a strict client-side memory ceiling ($\le 50\text{MB}$ for 50 sensors) across indefinite 24/7 operation.

## User Stories

1. As a backend developer, I want `TelemetryGatewayController` to call a single deep `TelemetryStorageEngine` interface, so that query routing, SQL dialects, and Arrow serialization details are completely hidden from the HTTP controller.
2. As a data pipeline engineer, I want Flink to deserialize incoming Protobuf records and track event-time watermarks only once before branching into Iceberg and rollup sinks, so that pipeline CPU and GC pressure are reduced by up to 66%.
3. As a frontend developer, I want `TelemetryChart.vue` to be a lightweight, presentational WebGL view component that consumes `useTelemetryDashboard`, so that visual styling is decoupled from streaming transport and semantic zoom state machines.
4. As a test engineer, I want to verify all multi-sensor zoom state transitions and live tailing synchronization in a headless environment without mocking WebGL canvas contexts, so that test suites run faster and with higher fidelity.
5. As an operations monitor, I want the web dashboard to run continuously 24/7 without memory growth or GC frame drops, so that mission-critical industrial telemetry monitoring remains stable over weeks of runtime.

## Implementation Decisions

- **Backend Query Seam**:
  - Introduce `com.telemetry.gateway.engine.TelemetryStorageEngine` presenting a single method `TelemetryStreamResult streamTelemetry(TelemetryQueryRequest request)`.
  - `TelemetryQueryRequest` encapsulates `sensorIds`, `timeRange`, and viewport `width`.
  - `TelemetryStreamResult` encapsulates `StreamingResponseBody`, target table name, query mode (`raw` / `am4`), and performance metadata.
  - `TelemetryGatewayController` is reduced to a concise 5-line controller delegating directly to the engine seam.
- **Unified Flink Pipeline Topology**:
  - Replace standalone `buildPipeline`, `build1HourPipeline`, and `buildIcebergRawStream` methods with a unified `TelemetryPipelineTopology.buildTopology(env, rawProtobufStream, sinkConfig)`.
  - A single `WatermarkStrategy` and `ProtobufDeserializer` feeds the three distinct pipelines via branch streams, eliminating redundant deserialization passes.
- **Frontend Headless Coordinator**:
  - Create `frontend/src/composables/useTelemetryDashboard.ts`.
  - Exposes simple declarative inputs and outputs: `activeSensors`, `timeWindow`, `isLiveTailActive`, `zoomTo(startMs, endMs)`, `resetToMacro()`, `toggleLiveTail()`, and reactive series hooks.
  - Internally manages `useTelemetryStream`, `useLiveTelemetryTail`, `AbortController` cancellation, 120ms debounce, and `isProgrammaticUpdate` concurrency locks.
- **Fixed-Budget Series Ring Buffer**:
  - Implement `CircularTelemetryBuffer` with a maximum sample capacity per series (e.g. 10,000 points per sensor).
  - Automatically evicts oldest live samples when capacity is reached, preventing unbounded heap expansion.

## Testing Decisions

- **Backend Storage Engine Seam**:
  - Test `TelemetryStorageEngine` with test doubles and integration tests against synthetic datasets.
  - Verify routing correctness for micro-zoom ($\le 6\text{h}$), raw ($\le 2W\text{s}$), intermediate (1m), and macro (1h) ranges through the single entry point.
- **Unified Pipeline Seam**:
  - Test `TelemetryPipelineTopology` using Flink `MiniCluster`, asserting that all three sink streams receive mathematically accurate records from a single input stream.
- **Headless Dashboard Composable**:
  - Vitest tests covering `useTelemetryDashboard` without WebGL / LightningChart dependencies.
  - Verify range navigation, debounce behavior, abort handling, and live sample merging in pure TypeScript.

## Out of Scope

- Introducing new third-party databases or cloud SaaS providers.
- Altering the ClickHouse 8-scalar column schema or Iceberg partition layout.
- Modifications to the Protobuf wire format.

## Further Notes

- Guided by codebase-design deep module principles (Leverage, Locality, and Clear Seams).
- Preserves full backward compatibility with existing tests and APIs.
