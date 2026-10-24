package com.telemetry.gateway;

/**
 * Semantic value object representing a query time window in epoch seconds.
 */
public record TimeRange(long lbSec, long ubSec) {
    public TimeRange {
        if (ubSec <= lbSec) {
            ubSec = lbSec + 60;
        }
    }

    public long durationSec() {
        return ubSec - lbSec;
    }

    /**
     * Macro viewing range (> 7 days, e.g. 30 days) routed to iot.sensor_rollup_1h.
     */
    public boolean isMacro30Day() {
        return durationSec() > 7L * 86400L;
    }

    /**
     * Intermediate viewing range (6 hours < ΔT <= 7 days) routed to iot.sensor_rollup_1m.
     */
    public boolean isIntermediate1m() {
        return durationSec() > 6L * 3600L && durationSec() <= 7L * 86400L;
    }

    /**
     * Micro-zoom range (ΔT <= 6 hours) routed to ClickHouse Iceberg raw table iot.sensor_iceberg_raw.
     */
    public boolean isMicroZoomIceberg() {
        return durationSec() <= 6L * 3600L;
    }

    /**
     * High-detail raw range (ΔT <= 2 * width in seconds) where downsampling is skipped
     * and raw 1-second measurements are returned directly.
     */
    public boolean isRawIceberg(int width) {
        return durationSec() <= (2L * Math.max(1, width));
    }

    public String targetTable() {
        if (isMicroZoomIceberg()) {
            return "iot.sensor_iceberg_raw";
        }
        return isMacro30Day() ? "iot.sensor_rollup_1h" : "iot.sensor_rollup_1m";
    }
}
