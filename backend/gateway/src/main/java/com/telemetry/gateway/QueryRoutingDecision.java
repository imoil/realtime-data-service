package com.telemetry.gateway;

import java.util.List;

/**
 * Encapsulates the table target and ClickHouse AM4 query for a given query request.
 */
public record QueryRoutingDecision(
        String targetTable,
        boolean isMacro30Day,
        String sqlQuery,
        TimeRange timeRange,
        List<Integer> sensorIds,
        int width
) {}
