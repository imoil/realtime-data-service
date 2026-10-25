package com.telemetry.pipeline.deserializer;

import com.telemetry.pipeline.DeviceBatchPayload;
import com.telemetry.pipeline.DeviceSensorMetric;
import com.telemetry.pipeline.TelemetryRecord;
import com.telemetry.pipeline.resolver.SensorSerialResolver;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class FastTelemetryProtobufDeserializerTest {

    private SensorSerialResolver resolver;
    private FastTelemetryProtobufDeserializer deserializer;

    @BeforeEach
    void setUp() {
        resolver = new SensorSerialResolver();
        resolver.registerDeviceRange("DEVICE-001", 100, 10001);
        resolver.registerMapping("TURBINE-01", 0, 20001);
        deserializer = new FastTelemetryProtobufDeserializer(resolver);
    }

    private static class RecordingCollector implements Collector<TelemetryRecord> {
        final List<TelemetryRecord> records = new ArrayList<>();

        @Override
        public void collect(TelemetryRecord record) {
            records.add(record);
        }

        @Override
        public void close() {}
    }

    @Test
    @DisplayName("FastTelemetryProtobufDeserializer가 단일 레코드를 자동 감지하여 단일 TelemetryRecord로 방출해야 한다")
    void testDetectsSingleRecordFastPath() throws IOException {
        TelemetryRecord single = new TelemetryRecord(10042, 1729800000000L, 42.5);
        byte[] payload = single.toProtobufBytes();

        RecordingCollector collector = new RecordingCollector();
        ConsumerRecord<byte[], byte[]> kafkaRecord = new ConsumerRecord<>(
                "iot.telemetry.raw", 0, 1L, null, payload
        );

        deserializer.deserialize(kafkaRecord, collector);

        assertThat(collector.records).hasSize(1);
        TelemetryRecord result = collector.records.get(0);
        assertThat(result.getSensorId()).isEqualTo(10042);
        assertThat(result.getTimestampMs()).isEqualTo(1729800000000L);
        assertThat(result.getValue()).isEqualTo(42.5);
    }

    @Test
    @DisplayName("FastTelemetryProtobufDeserializer가 배치 페이로드를 자동 감지하여 평탄화된 100개 TelemetryRecord 스트림으로 언롤링해야 한다")
    void testDetectsBatchPayloadAndUnrollsMetrics() throws IOException {
        int sensorCount = 100;
        long timestampMs = 1729800000000L;

        List<DeviceSensorMetric> metrics = new ArrayList<>(sensorCount);
        for (int i = 0; i < sensorCount; i++) {
            metrics.add(new DeviceSensorMetric(i, 50.0 + i, (i % 2 == 0) ? 0 : 1));
        }

        DeviceBatchPayload batch = new DeviceBatchPayload("DEVICE-001", timestampMs, metrics);
        byte[] payload = batch.toProtobufBytes();

        RecordingCollector collector = new RecordingCollector();
        ConsumerRecord<byte[], byte[]> kafkaRecord = new ConsumerRecord<>(
                "iot.telemetry.raw", 0, 2L, null, payload
        );

        deserializer.deserialize(kafkaRecord, collector);

        assertThat(collector.records).hasSize(sensorCount);

        for (int i = 0; i < sensorCount; i++) {
            TelemetryRecord record = collector.records.get(i);
            assertThat(record.getSensorId()).isEqualTo(10001 + i);
            assertThat(record.getTimestampMs()).isEqualTo(timestampMs);
            assertThat(record.getValue()).isEqualTo(50.0 + i);
            assertThat(record.getState()).isEqualTo((i % 2 == 0) ? 0 : 1);
            assertThat(record.getDeviceId()).isEqualTo("DEVICE-001");
            assertThat(record.getSensorIndex()).isEqualTo(i);
        }
    }

    @Test
    @DisplayName("카프카 헤더에서 deviceId 및 timestamp가 추출되어 미등록 장치도 결정론적 일련번호로 매핑되어야 한다")
    void testKafkaHeaderExtractionAndFallbackResolution() throws IOException {
        RecordHeaders headers = new RecordHeaders();
        headers.add(new RecordHeader(FastTelemetryProtobufDeserializer.HEADER_DEVICE_ID, "DEVICE-005".getBytes(StandardCharsets.UTF_8)));
        headers.add(new RecordHeader(FastTelemetryProtobufDeserializer.HEADER_TIMESTAMP, "1729800100000".getBytes(StandardCharsets.UTF_8)));

        List<DeviceSensorMetric> metrics = List.of(new DeviceSensorMetric(0, 99.9));
        DeviceBatchPayload batch = new DeviceBatchPayload("", 0L, metrics);
        byte[] payload = batch.toProtobufBytes();

        RecordingCollector collector = new RecordingCollector();
        ConsumerRecord<byte[], byte[]> kafkaRecord = new ConsumerRecord<>(
                "iot.telemetry.raw", 0, 3L, 0L, org.apache.kafka.common.record.TimestampType.CREATE_TIME,
                0, 0, null, payload, headers, null
        );

        deserializer.deserialize(kafkaRecord, collector);

        assertThat(collector.records).hasSize(1);
        TelemetryRecord result = collector.records.get(0);

        assertThat(result.getSensorId()).isEqualTo(14001);
        assertThat(result.getTimestampMs()).isEqualTo(1729800100000L);
        assertThat(result.getValue()).isEqualTo(99.9);
        assertThat(result.getDeviceId()).isEqualTo("DEVICE-005");
    }

    @Test
    @DisplayName("null 또는 빈 레코드는 예외 없이 무시되어야 한다")
    void testNullOrEmptyPayloadHandling() throws IOException {
        RecordingCollector collector = new RecordingCollector();

        deserializer.deserialize(null, collector);
        deserializer.deserialize(new ConsumerRecord<>("t", 0, 0, null, null), collector);
        deserializer.deserialize(new ConsumerRecord<>("t", 0, 0, null, new byte[0]), collector);

        assertThat(collector.records).isEmpty();
    }
}
