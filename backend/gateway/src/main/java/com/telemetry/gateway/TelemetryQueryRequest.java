package com.telemetry.gateway;

import java.util.Arrays;
import java.util.List;

/**
 * Value object representing a high-performance telemetry query request.
 */
public record TelemetryQueryRequest(
        List<Integer> sensorIds,
        TimeRange timeRange,
        int width
) {
    public TelemetryQueryRequest {
        if (sensorIds == null || sensorIds.isEmpty()) {
            sensorIds = List.of(1);
        }
        if (timeRange == null) {
            timeRange = new TimeRange(1729728000L, 1729728060L);
        }
        if (width <= 0) {
            width = 1920;
        }
    }

    public static TelemetryQueryRequest of(
            String sensorIdsParam,
            int defaultSensorId,
            long lbSec,
            long ubSec,
            int width
    ) {
        List<Integer> ids;
        if (sensorIdsParam != null && !sensorIdsParam.isBlank()) {
            ids = Arrays.stream(sensorIdsParam.split(","))
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .map(Integer::parseInt)
                    .toList();
        } else {
            ids = List.of(defaultSensorId);
        }
        return new TelemetryQueryRequest(ids, new TimeRange(lbSec, ubSec), width);
    }
}
