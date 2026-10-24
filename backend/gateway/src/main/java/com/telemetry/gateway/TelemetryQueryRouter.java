package com.telemetry.gateway;

import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Dynamic query router that targets iot.sensor_rollup_1h for macro ranges (>7d)
 * and iot.sensor_rollup_1m for intermediate ranges.
 */
@Component
public class TelemetryQueryRouter {

    public QueryRoutingDecision resolve(TimeRange timeRange, List<Integer> sensorIds, int width) {
        if (width <= 0) {
            width = 1920;
        }
        if (sensorIds == null || sensorIds.isEmpty()) {
            sensorIds = List.of(1);
        }

        String targetTable = timeRange.targetTable();
        boolean isMacro = timeRange.isMacro30Day();

        String sensorListStr = sensorIds.stream()
                .map(String::valueOf)
                .collect(Collectors.joining(", "));

        String sqlQuery = String.format("""
                SELECT
                    toUInt32(sensor_id) AS sensor_id,
                    toFloat64(point.1)  AS ts_ms,
                    toFloat64(point.2)  AS val
                FROM (
                    SELECT
                        sensor_id,
                        round(%d * (toUnixTimestamp(bucket_ts) - %d) / (%d - %d)) AS pixel_bin,
                        arrayDistinct(arraySort(x -> x.1, [
                            (toUnixTimestamp64Milli(min(first_ts)),   argMin(first_val, first_ts)),
                            (toUnixTimestamp64Milli(argMin(min_val_ts, min_val)), min(min_val)),
                            (toUnixTimestamp64Milli(argMax(max_val_ts, max_val)), max(max_val)),
                            (toUnixTimestamp64Milli(max(last_ts)),    argMax(last_val, last_ts))
                        ])) AS am4_points
                    FROM %s
                    WHERE sensor_id IN (%s)
                      AND bucket_ts >= toDateTime(%d)
                      AND bucket_ts <= toDateTime(%d)
                    GROUP BY sensor_id, pixel_bin
                )
                ARRAY JOIN am4_points AS point
                ORDER BY sensor_id ASC, ts_ms ASC
                FORMAT ArrowStream;
                """,
                width, timeRange.lbSec(), timeRange.ubSec(), timeRange.lbSec(),
                targetTable, sensorListStr, timeRange.lbSec(), timeRange.ubSec()
        );

        return new QueryRoutingDecision(targetTable, isMacro, sqlQuery, timeRange, sensorIds, width);
    }
}
