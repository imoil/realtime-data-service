package com.telemetry.pipeline;

import com.google.protobuf.CodedOutputStream;
import com.telemetry.pipeline.deserializer.FastTelemetryProtobufDeserializer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ProtobufCompatibilityTest {

    @Test
    @DisplayName("Protobuf binary backward compatibility: legacy single-record byte streams deserialize without errors")
    void testLegacySingleRecordBackwardCompatibility() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        CodedOutputStream output = CodedOutputStream.newInstance(baos);
        output.writeUInt32(1, 10042);
        output.writeInt64(2, 1729800000000L);
        output.writeDouble(3, 42.5);
        output.flush();
        byte[] legacyBytes = baos.toByteArray();

        TelemetryRecord record = TelemetryRecord.fromProtobufBytes(legacyBytes);
        assertThat(record.getSensorId()).isEqualTo(10042);
        assertThat(record.getTimestampMs()).isEqualTo(1729800000000L);
        assertThat(record.getValue()).isEqualTo(42.5);

        FastTelemetryProtobufDeserializer deserializer = new FastTelemetryProtobufDeserializer();
        List<TelemetryRecord> collectorList = new ArrayList<>();
        Collector<TelemetryRecord> collector = new Collector<>() {
            @Override
            public void collect(TelemetryRecord record) {
                collectorList.add(record);
            }

            @Override
            public void close() {}
        };

        ConsumerRecord<byte[], byte[]> kafkaRecord = new ConsumerRecord<>(
                "iot.telemetry.raw", 0, 0L, null, legacyBytes
        );

        deserializer.deserialize(kafkaRecord, collector);

        assertThat(collectorList).hasSize(1);
        TelemetryRecord deserialized = collectorList.get(0);
        assertThat(deserialized.getSensorId()).isEqualTo(10042);
        assertThat(deserialized.getTimestampMs()).isEqualTo(1729800000000L);
        assertThat(deserialized.getValue()).isEqualTo(42.5);
    }

    @Test
    @DisplayName("DeviceBatchPayload yields at least 56.7% Kafka ingress bandwidth reduction compared to single records")
    void testDeviceBatchBandwidthReduction() {
        int sensorCount = 100;
        long timestampMs = 1729800000000L;

        int totalSingleBytes = 0;
        for (int i = 0; i < sensorCount; i++) {
            TelemetryRecord single = new TelemetryRecord(10001 + i, timestampMs, 25.0 + (i * 0.1));
            byte[] bytes = single.toProtobufBytes();
            totalSingleBytes += bytes.length;
        }

        List<DeviceSensorMetric> metrics = new ArrayList<>(sensorCount);
        for (int i = 0; i < sensorCount; i++) {
            metrics.add(new DeviceSensorMetric(i, 25.0 + (i * 0.1)));
        }
        DeviceBatchPayload batch = new DeviceBatchPayload("DEVICE-001", timestampMs, metrics);
        byte[] batchBytes = batch.toProtobufBytes();

        int kafkaFramingOverheadPerRecord = 14;
        int totalKafkaSingleIngressBytes = totalSingleBytes + (sensorCount * kafkaFramingOverheadPerRecord);
        int totalKafkaBatchIngressBytes = batchBytes.length + kafkaFramingOverheadPerRecord;
        double reductionPercentage = ((double) (totalKafkaSingleIngressBytes - totalKafkaBatchIngressBytes) / totalKafkaSingleIngressBytes) * 100.0;

        System.out.printf("100 Single Records Ingress: %d bytes%n", totalKafkaSingleIngressBytes);
        System.out.printf("DeviceBatchPayload Ingress: %d bytes%n", totalKafkaBatchIngressBytes);
        System.out.printf("Kafka Ingress Bandwidth Reduction: %.2f%%%n", reductionPercentage);

        assertThat(reductionPercentage).isGreaterThanOrEqualTo(56.7);
    }

    @Test
    @DisplayName("DeviceBatchPayload round-trip serialization and deserialization produces identical data")
    void testDeviceBatchRoundTripSerialization() {
        long timestamp = 1729800000000L;
        List<DeviceSensorMetric> metrics = List.of(
                new DeviceSensorMetric(0, 10.5, 0),
                new DeviceSensorMetric(1, 20.75, 1),
                new DeviceSensorMetric(2, 30.125, 2)
        );
        DeviceBatchPayload original = new DeviceBatchPayload("TURBINE-42", timestamp, metrics);
        byte[] bytes = original.toProtobufBytes();

        DeviceBatchPayload deserialized = DeviceBatchPayload.fromProtobufBytes(bytes);
        assertThat(deserialized.getDeviceId()).isEqualTo("TURBINE-42");
        assertThat(deserialized.getTimestampMs()).isEqualTo(timestamp);
        assertThat(deserialized.getMetricCount()).isEqualTo(3);
        assertThat(deserialized.getMetrics()).isEqualTo(metrics);
        assertThat(deserialized).isEqualTo(original);
    }
}
