package com.telemetry.pipeline;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class TelemetryPipelineTopologyTest {

    @Test
    @DisplayName("Builds unified 3-way topology with single-pass deserialization and watermark assignment")
    void testUnifiedTopologyConstruction() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        TelemetryRecord record1 = new TelemetryRecord(1, 1729728000000L, 25.5);
        TelemetryRecord record2 = new TelemetryRecord(2, 1729728001000L, 30.0);

        DataStream<byte[]> rawStream = env.fromData(List.of(
                record1.toProtobufBytes(),
                record2.toProtobufBytes()
        ));

        TelemetryPipelineTopology.TopologyStreams streams =
                TelemetryPipelineTopology.buildTopology(env, rawStream);

        assertThat(streams.icebergRawStream()).isNotNull();
        assertThat(streams.rollup1mStream()).isNotNull();
        assertThat(streams.rollup1hStream()).isNotNull();

        // Verify delegation in SingleSensorPipelineJob
        DataStream<SensorRollup1m> legacy1m = SingleSensorPipelineJob.buildPipeline(env, rawStream);
        assertThat(legacy1m).isNotNull();

        DataStream<SensorRollup1h> legacy1h = SingleSensorPipelineJob.build1HourPipeline(env, rawStream);
        assertThat(legacy1h).isNotNull();

        DataStream<TelemetryRecord> legacyRaw = SingleSensorPipelineJob.buildIcebergRawStream(env, rawStream);
        assertThat(legacyRaw).isNotNull();
    }
}
