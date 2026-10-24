package com.telemetry.pipeline;

import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TelemetryStreamJobTest {

    @Test
    @DisplayName("TelemetryJobParameters parses custom CLI arguments and fallback defaults")
    void testParameterParsing() {
        String[] args = new String[]{
                "--kafka.bootstrap.servers=kafka-prod:9092",
                "--kafka.topic=custom.telemetry.raw",
                "--kafka.group.id=custom-group",
                "--checkpoint.interval.ms=30000",
                "--clickhouse.url=http://clickhouse-prod:8123"
        };

        TelemetryJobParameters params = TelemetryJobParameters.fromArgs(args);

        assertThat(params.getKafkaBootstrapServers()).isEqualTo("kafka-prod:9092");
        assertThat(params.getKafkaTopic()).isEqualTo("custom.telemetry.raw");
        assertThat(params.getKafkaGroupId()).isEqualTo("custom-group");
        assertThat(params.getCheckpointIntervalMs()).isEqualTo(30_000L);
        assertThat(params.getClickhouseUrl()).isEqualTo("http://clickhouse-prod:8123");

        // Defaults
        assertThat(params.getIcebergCatalogUri()).isEqualTo(TelemetryJobParameters.DEFAULT_ICEBERG_CATALOG);
        assertThat(params.getIcebergWarehouse()).isEqualTo(TelemetryJobParameters.DEFAULT_ICEBERG_WAREHOUSE);
        assertThat(params.getClickhouseDatabase()).isEqualTo(TelemetryJobParameters.DEFAULT_CLICKHOUSE_DATABASE);
    }

    @Test
    @DisplayName("Builds KafkaSource builder with specified parameters")
    void testBuildKafkaSource() {
        TelemetryJobParameters params = TelemetryJobParameters.fromArgs(new String[]{
                "--kafka.bootstrap.servers=localhost:9092",
                "--kafka.topic=iot.telemetry.raw"
        });

        KafkaSource<byte[]> source = TelemetryStreamJob.buildKafkaSource(params);
        assertThat(source).isNotNull();
    }

    @Test
    @DisplayName("RawByteArrayDeserializer returns unchanged byte payload")
    void testRawByteArrayDeserializer() {
        TelemetryStreamJob.RawByteArrayDeserializer deserializer = new TelemetryStreamJob.RawByteArrayDeserializer();
        byte[] sample = new byte[]{1, 2, 3, 4};

        assertThat(deserializer.deserialize(sample)).isEqualTo(sample);
        assertThat(deserializer.isEndOfStream(sample)).isFalse();
        assertThat(deserializer.getProducedType()).isNotNull();
    }

    @Test
    @DisplayName("Configures complete Flink execution graph attaching all 3 production sinks")
    void testConfigurePipelineExecutionGraph() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        TelemetryJobParameters params = TelemetryJobParameters.fromArgs(new String[]{});

        byte[] sampleRecord = new TelemetryRecord(1, 1729728000000L, 25.0).toProtobufBytes();
        DataStream<byte[]> inputStream = env.fromData(sampleRecord, sampleRecord);

        TelemetryPipelineTopology.TopologyStreams streams =
                TelemetryStreamJob.configurePipeline(env, inputStream, params);

        assertThat(streams.icebergRawStream()).isNotNull();
        assertThat(streams.rollup1mStream()).isNotNull();
        assertThat(streams.rollup1hStream()).isNotNull();

        String executionPlan = env.getExecutionPlan();
        assertThat(executionPlan).contains("SinglePassProtobufDeserializer");
        assertThat(executionPlan).contains("SinglePassWatermarkAssigner");
        assertThat(executionPlan).contains("IcebergRawSinkNode");
        assertThat(executionPlan).contains("ClickHouse1mSinkNode");
        assertThat(executionPlan).contains("ClickHouse1hSinkNode");
    }
}
