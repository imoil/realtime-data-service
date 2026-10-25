package com.telemetry.pipeline;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.WireFormat;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Batched device telemetry payload for multi-sensor IoT edge transmissions.
 * Yields significant Kafka ingress bandwidth reduction by amortizing device identifier
 * and timestamp metadata across multiple sensor readings in a single Protobuf wire message.
 */
public class DeviceBatchPayload implements Serializable {
    private static final long serialVersionUID = 1L;

    public static final int FIELD_DEVICE_ID = 1;
    public static final int FIELD_TIMESTAMP_MS = 2;
    public static final int FIELD_METRICS = 3;

    private final String deviceId;
    private final long timestampMs;
    private final List<DeviceSensorMetric> metrics;

    public DeviceBatchPayload(String deviceId, long timestampMs, List<DeviceSensorMetric> metrics) {
        this.deviceId = (deviceId != null) ? deviceId.trim() : "";
        this.timestampMs = timestampMs;
        this.metrics = (metrics != null) ? Collections.unmodifiableList(new ArrayList<>(metrics)) : Collections.emptyList();
    }

    public String getDeviceId() {
        return !deviceId.isBlank() ? deviceId : "DEVICE-DEFAULT";
    }

    public long getTimestampMs() {
        return timestampMs;
    }

    public List<DeviceSensorMetric> getMetrics() {
        return metrics;
    }

    public int getMetricCount() {
        return metrics.size();
    }

    /**
     * Serializes this batched device payload into standard Protobuf wire format.
     */
    public byte[] toProtobufBytes() {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            CodedOutputStream output = CodedOutputStream.newInstance(baos);

            if (!deviceId.isBlank()) {
                output.writeString(FIELD_DEVICE_ID, deviceId);
            }
            if (timestampMs > 0) {
                output.writeInt64(FIELD_TIMESTAMP_MS, timestampMs);
            }

            for (DeviceSensorMetric metric : metrics) {
                int metricSize = metric.computeSerializedSize();
                output.writeTag(FIELD_METRICS, WireFormat.WIRETYPE_LENGTH_DELIMITED);
                output.writeUInt32NoTag(metricSize);
                metric.writeTo(output);
            }

            output.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize DeviceBatchPayload to Protobuf bytes", e);
        }
    }

    /**
     * Parses a DeviceBatchPayload from raw Protobuf wire bytes.
     */
    public static DeviceBatchPayload fromProtobufBytes(byte[] bytes) {
        try {
            CodedInputStream input = CodedInputStream.newInstance(bytes);
            String deviceId = "";
            long timestampMs = 0L;
            List<DeviceSensorMetric> metrics = new ArrayList<>();

            while (!input.isAtEnd()) {
                int tag = input.readTag();
                if (tag == 0) break;
                int fieldNumber = tag >>> 3;
                switch (fieldNumber) {
                    case FIELD_DEVICE_ID -> deviceId = input.readString();
                    case FIELD_TIMESTAMP_MS -> timestampMs = input.readInt64();
                    case FIELD_METRICS -> {
                        int length = input.readRawVarint32();
                        int oldLimit = input.pushLimit(length);
                        metrics.add(DeviceSensorMetric.parseFrom(input));
                        input.popLimit(oldLimit);
                    }
                    default -> input.skipField(tag);
                }
            }
            return new DeviceBatchPayload(deviceId, timestampMs, metrics);
        } catch (IOException e) {
            throw new RuntimeException("Failed to parse DeviceBatchPayload from Protobuf bytes", e);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DeviceBatchPayload that)) return false;
        return timestampMs == that.timestampMs &&
                Objects.equals(getDeviceId(), that.getDeviceId()) &&
                Objects.equals(metrics, that.metrics);
    }

    @Override
    public int hashCode() {
        return Objects.hash(getDeviceId(), timestampMs, metrics);
    }

    @Override
    public String toString() {
        return "DeviceBatchPayload{" +
                "deviceId='" + getDeviceId() + '\'' +
                ", timestampMs=" + timestampMs +
                ", metricCount=" + metrics.size() +
                '}';
    }
}
