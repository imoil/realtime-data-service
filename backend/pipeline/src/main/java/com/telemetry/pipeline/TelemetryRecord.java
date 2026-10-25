package com.telemetry.pipeline;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Objects;

/**
 * Single-sensor telemetry reading.
 * Provides wire-level Protobuf encoding and decoding for synthetic generation
 * and edge stream processing with 100% backward compatibility for tags 1-3.
 */
public class TelemetryRecord implements Serializable {
    private static final long serialVersionUID = 1L;

    // Protobuf Field Tags
    public static final int FIELD_SENSOR_ID = 1;
    public static final int FIELD_TIMESTAMP_MS = 2;
    public static final int FIELD_VALUE = 3;
    public static final int FIELD_STATE = 4;
    public static final int FIELD_DEVICE_ID = 5;
    public static final int FIELD_SENSOR_INDEX = 6;
    public static final int FIELD_DEVICE_NUMERIC_ID = 7;

    private final int sensorId;
    private final long timestampMs;
    private final double value;
    private final int state;
    private final String deviceId;
    private final int sensorIndex;
    private final long deviceNumericId;

    public TelemetryRecord(int sensorId, long timestampMs, double value) {
        this(sensorId, timestampMs, value, 0, null, -1, 0L);
    }

    public TelemetryRecord(int sensorId, long timestampMs, double value, int state, String deviceId, int sensorIndex) {
        this(sensorId, timestampMs, value, state, deviceId, sensorIndex, 0L);
    }

    public TelemetryRecord(int sensorId, long timestampMs, double value, int state, String deviceId, int sensorIndex, long deviceNumericId) {
        this.sensorId = sensorId;
        this.timestampMs = timestampMs;
        this.value = value;
        this.state = state;
        this.deviceId = deviceId;
        this.sensorIndex = sensorIndex;
        this.deviceNumericId = deviceNumericId;
    }

    public int getSensorId() {
        return sensorId;
    }

    public long getTimestampMs() {
        return timestampMs;
    }

    public double getValue() {
        return value;
    }

    public int getState() {
        return state;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public int getSensorIndex() {
        return sensorIndex;
    }

    public long getDeviceNumericId() {
        return deviceNumericId;
    }

    /**
     * Serializes this record into Protobuf wire format.
     * Preserves exact byte-level backward compatibility for tags 1-3.
     */
    public byte[] toProtobufBytes() {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            CodedOutputStream output = CodedOutputStream.newInstance(baos);
            output.writeUInt32(FIELD_SENSOR_ID, sensorId);
            output.writeInt64(FIELD_TIMESTAMP_MS, timestampMs);
            output.writeDouble(FIELD_VALUE, value);

            if (state != 0) {
                output.writeUInt32(FIELD_STATE, state);
            }
            if (deviceId != null && !deviceId.isEmpty()) {
                output.writeString(FIELD_DEVICE_ID, deviceId);
            }
            if (sensorIndex >= 0) {
                output.writeInt32(FIELD_SENSOR_INDEX, sensorIndex);
            }
            if (deviceNumericId > 0) {
                output.writeUInt32(FIELD_DEVICE_NUMERIC_ID, (int) deviceNumericId);
            }

            output.flush();
            return baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize TelemetryRecord to Protobuf bytes", e);
        }
    }

    /**
     * Parses a TelemetryRecord from raw Protobuf bytes.
     */
    public static TelemetryRecord fromProtobufBytes(byte[] bytes) {
        try {
            CodedInputStream input = CodedInputStream.newInstance(bytes);
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
                int fieldNumber = tag >>> 3;
                switch (fieldNumber) {
                    case FIELD_SENSOR_ID -> sensorId = input.readUInt32();
                    case FIELD_TIMESTAMP_MS -> timestampMs = input.readInt64();
                    case FIELD_VALUE -> value = input.readDouble();
                    case FIELD_STATE -> state = input.readUInt32();
                    case FIELD_DEVICE_ID -> deviceId = input.readString();
                    case FIELD_SENSOR_INDEX -> sensorIndex = input.readInt32();
                    case FIELD_DEVICE_NUMERIC_ID -> deviceNumericId = input.readUInt32();
                    default -> input.skipField(tag);
                }
            }
            return new TelemetryRecord(sensorId, timestampMs, value, state, deviceId, sensorIndex, deviceNumericId);
        } catch (IOException e) {
            throw new RuntimeException("Failed to parse TelemetryRecord from Protobuf bytes", e);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TelemetryRecord that)) return false;
        return sensorId == that.sensorId &&
                timestampMs == that.timestampMs &&
                Double.compare(that.value, value) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(sensorId, timestampMs, value);
    }

    @Override
    public String toString() {
        return "TelemetryRecord{" +
                "sensorId=" + sensorId +
                ", timestampMs=" + timestampMs +
                ", value=" + value +
                (state != 0 ? ", state=" + state : "") +
                (deviceId != null ? ", deviceId='" + deviceId + '\'' : "") +
                (sensorIndex >= 0 ? ", sensorIndex=" + sensorIndex : "") +
                (deviceNumericId > 0 ? ", deviceNumericId=" + deviceNumericId : "") +
                '}';
    }
}
