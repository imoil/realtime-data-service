package com.telemetry.pipeline;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;

import java.time.Duration;

/**
 * Unified stream processing topology for industrial telemetry.
 *
 * Ingests incoming Protobuf stream, performs deserialization and event-time
 * watermark assignment exactly once, and branches into three dedicated streams:
 * 1. Iceberg Raw Stream: bucket(32, sensor_id) partitioned raw stream
 * 2. 1-Minute Rollup Stream: tumbling event-time window with 8-scalar extrema
 * 3. 1-Hour Rollup Stream: tumbling event-time window with 8-scalar extrema
 */
public class TelemetryPipelineTopology {

    /**
     * Cohesive container holding all three branched pipeline streams.
     */
    public record TopologyStreams(
            DataStream<TelemetryRecord> icebergRawStream,
            DataStream<SensorRollup1m> rollup1mStream,
            DataStream<SensorRollup1h> rollup1hStream
    ) {}

    /**
     * Builds the unified 3-way streaming topology from a single raw Protobuf input stream.
     */
    public static TopologyStreams buildTopology(
            StreamExecutionEnvironment env,
            DataStream<byte[]> rawProtobufStream
    ) {
        WatermarkStrategy<TelemetryRecord> watermarkStrategy = WatermarkStrategy
                .<TelemetryRecord>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((record, timestamp) -> record.getTimestampMs());

        // Single pass: Deserialization & Watermark Assignment
        DataStream<TelemetryRecord> recordStream = rawProtobufStream
                .map(TelemetryRecord::fromProtobufBytes)
                .name("SinglePassProtobufDeserializer")
                .assignTimestampsAndWatermarks(watermarkStrategy)
                .name("SinglePassWatermarkAssigner");

        // Branch 1: Iceberg Raw Sink Stream (hash bucket routing by sensor_id % 32)
        IcebergSinkHelper sinkHelper = new IcebergSinkHelper();
        DataStream<TelemetryRecord> icebergRawStream = sinkHelper.routeByBucket(recordStream);

        // Branch 2: 1-Minute Rollup Stream
        DataStream<SensorRollup1m> rollup1mStream = recordStream
                .keyBy(TelemetryRecord::getSensorId)
                .window(TumblingEventTimeWindows.of(Duration.ofMinutes(1)))
                .aggregate(new SensorRollupExtremaAggregator())
                .name("1MinuteExtremaAggregator");

        // Branch 3: 1-Hour Rollup Stream
        DataStream<SensorRollup1h> rollup1hStream = recordStream
                .keyBy(TelemetryRecord::getSensorId)
                .window(TumblingEventTimeWindows.of(Duration.ofHours(1)))
                .aggregate(new SensorRollup1hExtremaAggregator())
                .name("1HourExtremaAggregator");

        return new TopologyStreams(icebergRawStream, rollup1mStream, rollup1hStream);
    }
}
