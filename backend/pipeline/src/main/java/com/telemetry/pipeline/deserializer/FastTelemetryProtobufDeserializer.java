package com.telemetry.pipeline.deserializer;

import com.google.protobuf.CodedInputStream;
import com.telemetry.pipeline.DeviceSensorMetric;
import com.telemetry.pipeline.TelemetryRecord;
import com.telemetry.pipeline.resolver.SensorSerialResolver;
import com.telemetry.pipeline.resolver.SensorSerialResolver.DeviceRoutingTable;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * High-throughput, zero-allocation Protobuf deserializer for Apache Flink 1.20.
 * Automatically detects single-point versus batched device telemetry payloads,
 * unrolls batched metrics into flat TelemetryRecord streams, and resolves
 * physical device sensor indices using the zero-allocation SensorSerialResolver.
 */
public class FastTelemetryProtobufDeserializer implements KafkaRecordDeserializationSchema<TelemetryRecord> {
    private static final long serialVersionUID = 1L;

    public static final String HEADER_DEVICE_ID = "deviceId";
    public static final String HEADER_TIMESTAMP = "timestamp";

    // Exact Wire Tags for Single-Point TelemetryRecord (fieldNumber << 3 | wireType)
    public static final int WIRE_SINGLE_SENSOR_ID = (1 << 3) | 0;          // 8
    public static final int WIRE_TIMESTAMP_MS = (2 << 3) | 0;              // 16
    public static final int WIRE_SINGLE_VALUE = (3 << 3) | 1;              // 25
    public static final int WIRE_SINGLE_STATE = (4 << 3) | 0;              // 32
    public static final int WIRE_SINGLE_DEVICE_ID = (5 << 3) | 2;          // 42
    public static final int WIRE_SINGLE_SENSOR_INDEX = (6 << 3) | 0;       // 48
    public static final int WIRE_SINGLE_DEVICE_NUMERIC_ID = (7 << 3) | 0;  // 56

    // Exact Wire Tags for Batched DevicePayload
    public static final int WIRE_BATCH_DEVICE_ID = (1 << 3) | 2;           // 10
    public static final int WIRE_BATCH_TIMESTAMP_MS = (2 << 3) | 0;         // 16
    public static final int WIRE_BATCH_METRICS = (3 << 3) | 2;             // 26

    private final SensorSerialResolver resolver;

    public FastTelemetryProtobufDeserializer() {
        this(SensorSerialResolver.getInstance());
    }

    public FastTelemetryProtobufDeserializer(SensorSerialResolver resolver) {
        this.resolver = (resolver != null) ? resolver : SensorSerialResolver.getInstance();
    }

    public SensorSerialResolver getResolver() {
        return resolver;
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<TelemetryRecord> out) throws IOException {
        if (record == null) {
            return;
        }
        byte[] payload = record.value();
        if (payload == null || payload.length == 0) {
            return;
        }

        int firstByte = payload[0] & 0xFF;
        if (firstByte == WIRE_SINGLE_SENSOR_ID) {
            deserializeSingleRecordFastPath(payload, out);
            return;
        }

        String deviceId = null;
        if (record.headers() != null) {
            Header devHeader = record.headers().lastHeader(HEADER_DEVICE_ID);
            if (devHeader != null && devHeader.value() != null) {
                deviceId = new String(devHeader.value(), StandardCharsets.UTF_8);
            }
        }
        if (deviceId == null && record.key() != null && record.key().length > 0) {
            deviceId = new String(record.key(), StandardCharsets.UTF_8);
        }

        long timestampMs = record.timestamp();
        if (record.headers() != null) {
            Header tsHeader = record.headers().lastHeader(HEADER_TIMESTAMP);
            if (tsHeader != null && tsHeader.value() != null) {
                try {
                    timestampMs = Long.parseLong(new String(tsHeader.value(), StandardCharsets.UTF_8));
                } catch (NumberFormatException ignored) {}
            }
        }
        if (timestampMs <= 0) {
            timestampMs = System.currentTimeMillis();
        }

        deserializePolymorphicPayload(payload, deviceId, timestampMs, out);
    }

    private void deserializeSingleRecordFastPath(byte[] payload, Collector<TelemetryRecord> out) throws IOException {
        CodedInputStream input = CodedInputStream.newInstance(payload);
        int sensorId = 0;
        long timestampMs = 0L;
        double value = 0.0;
        int state = 0;
        String deviceId = null;
        int sensorIndex = -1;
        long deviceNumericId = 0L;

        while (!input.isAtEnd()) {
            int tag = input.readTag();
            if (tag == 0) break;
            switch (tag) {
                case WIRE_SINGLE_SENSOR_ID -> sensorId = input.readUInt32();
                case WIRE_TIMESTAMP_MS -> timestampMs = input.readInt64();
                case WIRE_SINGLE_VALUE -> value = input.readDouble();
                case WIRE_SINGLE_STATE -> state = input.readUInt32();
                case WIRE_SINGLE_DEVICE_ID -> deviceId = input.readString();
                case WIRE_SINGLE_SENSOR_INDEX -> sensorIndex = input.readInt32();
                case WIRE_SINGLE_DEVICE_NUMERIC_ID -> deviceNumericId = input.readUInt32();
                default -> input.skipField(tag);
            }
        }

        if (sensorId <= 0 && deviceId != null && sensorIndex >= 0) {
            sensorId = resolver.resolve(deviceId, sensorIndex);
        }

        out.collect(new TelemetryRecord(sensorId, timestampMs, value, state, deviceId, sensorIndex, deviceNumericId));
    }

    private void deserializePolymorphicPayload(
            byte[] payload,
            String headerDeviceId,
            long headerTimestampMs,
            Collector<TelemetryRecord> out
    ) throws IOException {
        CodedInputStream input = CodedInputStream.newInstance(payload);
        String deviceId = headerDeviceId;
        long timestampMs = headerTimestampMs;

        int singleSensorId = 0;
        int singleSensorIndex = -1;
        double singleValue = 0.0;
        int singleState = 0;
        long singleNumericId = 0L;
        boolean isBatch = false;

        DeviceRoutingTable cachedTable = null;

        while (!input.isAtEnd()) {
            int tag = input.readTag();
            if (tag == 0) break;

            switch (tag) {
                case WIRE_BATCH_DEVICE_ID -> {
                    isBatch = true;
                    String bodyDev = input.readString();
                    if (bodyDev != null && !bodyDev.isBlank() && !"DEVICE-DEFAULT".equals(bodyDev)) {
                        deviceId = bodyDev.trim();
                        cachedTable = null;
                    }
                }
                case WIRE_BATCH_TIMESTAMP_MS -> {
                    long bodyTs = input.readInt64();
                    if (bodyTs > 0) {
                        timestampMs = bodyTs;
                    }
                }
                case WIRE_BATCH_METRICS -> {
                    isBatch = true;
                    if (cachedTable == null) {
                        cachedTable = resolver.getOrCreateDeviceTable(deviceId);
                    }
                    parseAndEmitMetric(input, cachedTable, deviceId, timestampMs, out);
                }
                case WIRE_SINGLE_SENSOR_ID -> singleSensorId = input.readUInt32();
                case WIRE_SINGLE_VALUE -> singleValue = input.readDouble();
                case WIRE_SINGLE_STATE -> singleState = input.readUInt32();
                case WIRE_SINGLE_DEVICE_ID -> {
                    String bodyDev = input.readString();
                    if (bodyDev != null && !bodyDev.isBlank() && !"DEVICE-DEFAULT".equals(bodyDev)) {
                        deviceId = bodyDev.trim();
                        cachedTable = null;
                    }
                }
                case WIRE_SINGLE_SENSOR_INDEX -> singleSensorIndex = input.readInt32();
                case WIRE_SINGLE_DEVICE_NUMERIC_ID -> singleNumericId = input.readUInt32();
                default -> input.skipField(tag);
            }
        }

        if (!isBatch) {
            int resolvedSensorId = singleSensorId;
            if (resolvedSensorId <= 0) {
                if (cachedTable == null) {
                    cachedTable = resolver.getOrCreateDeviceTable(deviceId);
                }
                resolvedSensorId = cachedTable.resolve(Math.max(0, singleSensorIndex));
            }
            out.collect(new TelemetryRecord(resolvedSensorId, timestampMs, singleValue, singleState, deviceId, singleSensorIndex, singleNumericId));
        }
    }

    private void parseAndEmitMetric(
            CodedInputStream input,
            DeviceRoutingTable table,
            String deviceId,
            long timestampMs,
            Collector<TelemetryRecord> out
    ) throws IOException {
        int length = input.readRawVarint32();
        int oldLimit = input.pushLimit(length);
        DeviceSensorMetric metric = DeviceSensorMetric.parseFrom(input);
        input.popLimit(oldLimit);

        int serialNumber = table.resolve(metric.getSensorIndex());
        out.collect(new TelemetryRecord(serialNumber, timestampMs, metric.getValue(), metric.getState(), deviceId, metric.getSensorIndex()));
    }

    @Override
    public TypeInformation<TelemetryRecord> getProducedType() {
        return TypeInformation.of(TelemetryRecord.class);
    }
}
