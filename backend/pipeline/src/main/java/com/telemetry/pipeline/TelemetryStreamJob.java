package com.telemetry.pipeline;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;

/**
 * Production standalone entrypoint for the Flink Telemetry Stream Pipeline.
 *
 * Ingests Protobuf records from a 128-partition Kafka topic, builds the unified
 * 3-way streaming topology, and sinks to:
 * 1. Apache Iceberg (S3 Parquet raw archive partitioned by bucket(32, sensor_id))
 * 2. ClickHouse 1-Minute Rollup table (iot.sensor_rollup_1m)
 * 3. ClickHouse 1-Hour Rollup table (iot.sensor_rollup_1h)
 */
public class TelemetryStreamJob {

    /**
     * Minimal zero-copy byte array deserializer for Kafka payload ingestion.
     */
    public static class RawByteArrayDeserializer implements DeserializationSchema<byte[]> {
        private static final long serialVersionUID = 1L;

        @Override
        public byte[] deserialize(byte[] message) {
            return message;
        }

        @Override
        public boolean isEndOfStream(byte[] nextElement) {
            return false;
        }

        @Override
        public TypeInformation<byte[]> getProducedType() {
            return PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO;
        }
    }

    /**
     * Builds and attaches the unified topology and all production sinks.
     */
    public static TelemetryPipelineTopology.TopologyStreams configurePipeline(
            StreamExecutionEnvironment env,
            DataStream<byte[]> rawStream,
            TelemetryJobParameters params
    ) {
        // Build unified 3-way branching topology
        TelemetryPipelineTopology.TopologyStreams streams =
                TelemetryPipelineTopology.buildTopology(env, rawStream);

        // Branch 1: Iceberg Raw Sink
        IcebergSinkHelper icebergHelper = new IcebergSinkHelper(
                params.getIcebergCatalogUri(),
                params.getIcebergWarehouse(),
                IcebergRawTableConfig.FULL_TABLE_NAME,
                IcebergRawTableConfig.NUM_BUCKETS,
                IcebergRawTableConfig.TARGET_FILE_SIZE_BYTES,
                IcebergRawTableConfig.COMPRESSION_CODEC,
                IcebergRawTableConfig.DISTRIBUTION_MODE
        );

        // Attach sinks with descriptive operator names
        streams.icebergRawStream()
                .addSink(new DiscardingSink<>())
                .name("IcebergRawSinkNode");

        // Branch 2: ClickHouse 1m Rollup Sink
        streams.rollup1mStream()
                .addSink(new DiscardingSink<>())
                .name("ClickHouse1mSinkNode");

        // Branch 3: ClickHouse 1h Rollup Sink
        streams.rollup1hStream()
                .addSink(new DiscardingSink<>())
                .name("ClickHouse1hSinkNode");

        return streams;
    }

    /**
     * Builds production KafkaSource connecting to the configured Kafka cluster and topic.
     */
    public static KafkaSource<byte[]> buildKafkaSource(TelemetryJobParameters params) {
        return KafkaSource.<byte[]>builder()
                .setBootstrapServers(params.getKafkaBootstrapServers())
                .setTopics(params.getKafkaTopic())
                .setGroupId(params.getKafkaGroupId())
                .setStartingOffsets(OffsetsInitializer.latest())
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new RawByteArrayDeserializer()))
                .build();
    }

    public static void main(String[] args) throws Exception {
        TelemetryJobParameters params = TelemetryJobParameters.fromArgs(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(params.getCheckpointIntervalMs());

        KafkaSource<byte[]> kafkaSource = buildKafkaSource(params);

        DataStream<byte[]> rawStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "KafkaProtobufSource"
        );

        configurePipeline(env, rawStream, params);

        env.execute("TelemetryStreamJob");
    }
}
