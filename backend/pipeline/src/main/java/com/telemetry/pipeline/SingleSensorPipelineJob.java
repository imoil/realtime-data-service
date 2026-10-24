package com.telemetry.pipeline;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * High-level entry point delegating to the unified {@link TelemetryPipelineTopology}.
 * Preserves full backward compatibility with existing tests and invocation patterns.
 */
public class SingleSensorPipelineJob {

    public static DataStream<SensorRollup1m> buildPipeline(
            StreamExecutionEnvironment env,
            DataStream<byte[]> rawProtobufStream
    ) {
        return TelemetryPipelineTopology.buildTopology(env, rawProtobufStream).rollup1mStream();
    }

    public static DataStream<SensorRollup1h> build1HourPipeline(
            StreamExecutionEnvironment env,
            DataStream<byte[]> rawProtobufStream
    ) {
        return TelemetryPipelineTopology.buildTopology(env, rawProtobufStream).rollup1hStream();
    }

    public static DataStream<TelemetryRecord> buildIcebergRawStream(
            StreamExecutionEnvironment env,
            DataStream<byte[]> rawProtobufStream
    ) {
        return TelemetryPipelineTopology.buildTopology(env, rawProtobufStream).icebergRawStream();
    }
}
