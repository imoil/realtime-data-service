package com.telemetry.gateway;

/**
 * Deep storage module interface for telemetry data retrieval.
 *
 * Encapsulates multi-tier storage routing (Iceberg Parquet vs. 1m vs. 1h rollups),
 * single-scan AM4 downsampling, raw pass-through, and Arrow IPC binary streaming.
 */
public interface TelemetryStorageEngine {

    /**
     * Executes a telemetry query and returns an Arrow IPC StreamingResponseBody
     * along with routing metadata.
     */
    TelemetryStreamResult streamTelemetry(TelemetryQueryRequest request);
}
