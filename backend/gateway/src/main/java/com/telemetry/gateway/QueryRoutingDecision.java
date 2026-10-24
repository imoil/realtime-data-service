package com.telemetry.gateway;

import java.util.List;

/**
 * Encapsulates the table target and ClickHouse query for a given telemetry request.
 */
public record QueryRoutingDecision(
        String targetTable,
        boolean isMacro30Day,
        String sqlQuery,
        TimeRange timeRange,
        List<Integer> sensorIds,
        int width,
        boolean isRaw
) {
    public QueryRoutingDecision(
            String targetTable,
            boolean isMacro30Day,
            String sqlQuery,
            TimeRange timeRange,
            List<Integer> sensorIds,
            int width
    ) {
        this(targetTable, isMacro30Day, sqlQuery, timeRange, sensorIds, width, false);
    }
}
