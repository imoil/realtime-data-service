package com.telemetry.pipeline;

import java.io.Serializable;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * ClickHouse sink helper for iot.sensor_rollup_1m and iot.sensor_rollup_1h tables.
 */
public class ClickHouseSinkHelper implements Serializable {
    private static final long serialVersionUID = 1L;

    public static final String CREATE_TABLE_1M_DDL = """
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

    public static final String CREATE_TABLE_1H_DDL = """
            CREATE TABLE IF NOT EXISTS iot.sensor_rollup_1h (
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
        return buildInsertSql("iot.sensor_rollup_1m", rollup.getSensorId(), rollup.getBucketTs(),
                rollup.getFirstTs(), rollup.getFirstVal(), rollup.getMinValTs(), rollup.getMinVal(),
                rollup.getMaxValTs(), rollup.getMaxVal(), rollup.getLastTs(), rollup.getLastVal());
    }

    public static String toInsertSql1h(SensorRollup1h rollup) {
        return buildInsertSql("iot.sensor_rollup_1h", rollup.getSensorId(), rollup.getBucketTs(),
                rollup.getFirstTs(), rollup.getFirstVal(), rollup.getMinValTs(), rollup.getMinVal(),
                rollup.getMaxValTs(), rollup.getMaxVal(), rollup.getLastTs(), rollup.getLastVal());
    }

    private static String buildInsertSql(
            String tableName,
            int sensorId,
            long bucketTs,
            long firstTs,
            double firstVal,
            long minValTs,
            double minVal,
            long maxValTs,
            double maxVal,
            long lastTs,
            double lastVal
    ) {
        String bucketStr = SECOND_FORMATTER.format(Instant.ofEpochSecond(bucketTs));
        String firstStr = MILLI_FORMATTER.format(Instant.ofEpochMilli(firstTs));
        String minStr = MILLI_FORMATTER.format(Instant.ofEpochMilli(minValTs));
        String maxStr = MILLI_FORMATTER.format(Instant.ofEpochMilli(maxValTs));
        String lastStr = MILLI_FORMATTER.format(Instant.ofEpochMilli(lastTs));

        return String.format(
                "INSERT INTO %s (sensor_id, bucket_ts, first_ts, first_val, min_val_ts, min_val, max_val_ts, max_val, last_ts, last_val) " +
                        "VALUES (%d, '%s', '%s', %f, '%s', %f, '%s', %f, '%s', %f)",
                tableName,
                sensorId,
                bucketStr,
                firstStr,
                firstVal,
                minStr,
                minVal,
                maxStr,
                maxVal,
                lastStr,
                lastVal
        );
    }
}
