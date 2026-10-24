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

    public boolean isMacro30Day() {
        return durationSec() > 7L * 86400L;
    }

    public String targetTable() {
        return isMacro30Day() ? "iot.sensor_rollup_1h" : "iot.sensor_rollup_1m";
    }
}
