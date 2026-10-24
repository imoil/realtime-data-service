package com.telemetry.pipeline;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;

import java.time.Duration;

/**
 * End-to-end Flink streaming pipeline for telemetry rollups.
 * Ingests Protobuf telemetry stream, applies event-time tumbling windows (1m and 1h),
 * and computes 8-scalar extrema rollups.
 */
public class SingleSensorPipelineJob {

    public static DataStream<SensorRollup1m> buildPipeline(
            StreamExecutionEnvironment env,
            DataStream<byte[]> rawProtobufStream
    ) {
        WatermarkStrategy<TelemetryRecord> watermarkStrategy = WatermarkStrategy
                .<TelemetryRecord>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((record, timestamp) -> record.getTimestampMs());

        return rawProtobufStream
                .map(TelemetryRecord::fromProtobufBytes)
                .name("ProtobufDeserializer")
                .assignTimestampsAndWatermarks(watermarkStrategy)
                .name("WatermarkAssigner")
                .keyBy(TelemetryRecord::getSensorId)
                .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
                .aggregate(new SensorRollupExtremaAggregator())
                .name("1MinuteExtremaAggregator");
    }

    public static DataStream<SensorRollup1h> build1HourPipeline(
            StreamExecutionEnvironment env,
            DataStream<byte[]> rawProtobufStream
    ) {
        WatermarkStrategy<TelemetryRecord> watermarkStrategy = WatermarkStrategy
                .<TelemetryRecord>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((record, timestamp) -> record.getTimestampMs());

        return rawProtobufStream
                .map(TelemetryRecord::fromProtobufBytes)
                .name("ProtobufDeserializer")
                .assignTimestampsAndWatermarks(watermarkStrategy)
                .name("WatermarkAssigner")
                .keyBy(TelemetryRecord::getSensorId)
                .window(TumblingEventTimeWindows.of(Duration.ofHours(1)))
                .aggregate(new SensorRollup1hExtremaAggregator())
                .name("1HourExtremaAggregator");
    }
}
