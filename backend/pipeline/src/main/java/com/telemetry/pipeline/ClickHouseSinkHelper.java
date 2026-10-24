package com.telemetry.pipeline;

import java.io.Serializable;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * ClickHouse sink helper for iot.sensor_rollup_1m table.
 */
public class ClickHouseSinkHelper implements Serializable {
    private static final long serialVersionUID = 1L;

    public static final String CREATE_TABLE_DDL = """
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
            """;

    private static final DateTimeFormatter SECOND_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneOffset.UTC);
    private static final DateTimeFormatter MILLI_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneOffset.UTC);

    public static String toInsertSql(SensorRollup1m rollup) {
        String bucketStr = SECOND_FORMATTER.format(Instant.ofEpochSecond(rollup.getBucketTs()));
        String firstStr = MILLI_FORMATTER.format(Instant.ofEpochMilli(rollup.getFirstTs()));
        String minStr = MILLI_FORMATTER.format(Instant.ofEpochMilli(rollup.getMinValTs()));
        String maxStr = MILLI_FORMATTER.format(Instant.ofEpochMilli(rollup.getMaxValTs()));
        String lastStr = MILLI_FORMATTER.format(Instant.ofEpochMilli(rollup.getLastTs()));

        return String.format(
                "INSERT INTO iot.sensor_rollup_1m (sensor_id, bucket_ts, first_ts, first_val, min_val_ts, min_val, max_val_ts, max_val, last_ts, last_val) " +
                        "VALUES (%d, '%s', '%s', %f, '%s', %f, '%s', %f, '%s', %f)",
                rollup.getSensorId(),
                bucketStr,
                firstStr,
                rollup.getFirstVal(),
                minStr,
                rollup.getMinVal(),
                maxStr,
                rollup.getMaxVal(),
                lastStr,
                rollup.getLastVal()
        );
    }
}
