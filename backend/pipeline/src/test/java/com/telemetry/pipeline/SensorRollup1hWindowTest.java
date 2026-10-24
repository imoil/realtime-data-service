package com.telemetry.pipeline;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class SensorRollup1hWindowTest {

    @Test
    @DisplayName("Aggregates 3600 1-second records into exact 1-hour 8-scalar extrema")
    void testCompute1HourExtremaRollup() {
        long baseTs = Instant.parse("2026-10-24T00:00:00Z").toEpochMilli();
        int sensorId = 10;

        List<TelemetryRecord> records = new ArrayList<>();
        // 3600 seconds in 1 hour
        for (int i = 0; i < 3600; i++) {
            long ts = baseTs + (i * 1000L);
            double val = 50.0 + (i % 10);
            if (i == 0) {
                val = 52.0; // first
            } else if (i == 600) {
                val = 5.2; // min at 10m
            } else if (i == 2400) {
                val = 198.5; // max at 40m
            } else if (i == 3599) {
                val = 49.8; // last
            }
            records.add(new TelemetryRecord(sensorId, ts, val));
        }

        SensorRollup1hExtremaAggregator aggregator = new SensorRollup1hExtremaAggregator();
        ExtremaAccumulator acc = aggregator.createAccumulator();

        for (TelemetryRecord record : records) {
            acc = aggregator.add(record, acc);
        }

        SensorRollup1h rollup = aggregator.getResult(acc);

        assertThat(rollup.getSensorId()).isEqualTo(sensorId);
        // Aligned to 1-hour (3600 seconds)
        assertThat(rollup.getBucketTs()).isEqualTo(baseTs / 1000L);

        // First point
        assertThat(rollup.getFirstTs()).isEqualTo(baseTs);
        assertThat(rollup.getFirstVal()).isEqualTo(52.0);

        // Min point
        assertThat(rollup.getMinValTs()).isEqualTo(baseTs + 600_000L);
        assertThat(rollup.getMinVal()).isEqualTo(5.2);

        // Max point
        assertThat(rollup.getMaxValTs()).isEqualTo(baseTs + 2_400_000L);
        assertThat(rollup.getMaxVal()).isEqualTo(198.5);

        // Last point
        assertThat(rollup.getLastTs()).isEqualTo(baseTs + 3_599_000L);
        assertThat(rollup.getLastVal()).isEqualTo(49.8);

        // Verify ClickHouse SQL formatting
        String insertSql = ClickHouseSinkHelper.toInsertSql1h(rollup);
        assertThat(insertSql).contains("iot.sensor_rollup_1h");
        assertThat(insertSql).contains("5.200000");
        assertThat(insertSql).contains("198.500000");
    }
}
