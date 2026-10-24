package com.telemetry.pipeline;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class SensorRollupWindowFunctionTest {

    @Test
    @DisplayName("Aggregates 60 1-second records into exact 8-scalar AM4 extrema")
    void testComputeExtremaRollup() {
        long baseTs = Instant.parse("2026-10-24T00:00:00Z").toEpochMilli();
        int sensorId = 42;

        List<TelemetryRecord> records = new ArrayList<>();
        // 60 points from 0s to 59s
        for (int i = 0; i < 60; i++) {
            long ts = baseTs + (i * 1000L);
            double val = 20.0 + (i % 5); // baseline variations 20.0 ~ 24.0
            if (i == 0) {
                val = 22.5; // first_val
            } else if (i == 15) {
                val = -10.5; // global min
            } else if (i == 42) {
                val = 105.8; // global max
            } else if (i == 59) {
                val = 18.2; // last_val
            }
            records.add(new TelemetryRecord(sensorId, ts, val));
        }

        SensorRollupExtremaAggregator aggregator = new SensorRollupExtremaAggregator();
        ExtremaAccumulator acc = aggregator.createAccumulator();

        for (TelemetryRecord record : records) {
            acc = aggregator.add(record, acc);
        }

        SensorRollup1m rollup = aggregator.getResult(acc);

        assertThat(rollup.getSensorId()).isEqualTo(sensorId);
        assertThat(rollup.getBucketTs()).isEqualTo(baseTs / 1000L);

        // First point
        assertThat(rollup.getFirstTs()).isEqualTo(baseTs);
        assertThat(rollup.getFirstVal()).isEqualTo(22.5);

        // Min point
        assertThat(rollup.getMinValTs()).isEqualTo(baseTs + 15000L);
        assertThat(rollup.getMinVal()).isEqualTo(-10.5);

        // Max point
        assertThat(rollup.getMaxValTs()).isEqualTo(baseTs + 42000L);
        assertThat(rollup.getMaxVal()).isEqualTo(105.8);

        // Last point
        assertThat(rollup.getLastTs()).isEqualTo(baseTs + 59000L);
        assertThat(rollup.getLastVal()).isEqualTo(18.2);
    }

    @Test
    @DisplayName("Encodes and decodes TelemetryRecord via binary Protobuf")
    void testProtobufSerializationRoundTrip() {
        TelemetryRecord original = new TelemetryRecord(101, 1729728000000L, 42.195);
        byte[] bytes = original.toProtobufBytes();
        assertThat(bytes).isNotEmpty();

        TelemetryRecord parsed = TelemetryRecord.fromProtobufBytes(bytes);
        assertThat(parsed).isEqualTo(original);
    }
}
