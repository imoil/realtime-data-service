package com.telemetry.pipeline;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Objects;

/**
 * Single-sensor telemetry reading.
 * Provides wire-level Protobuf encoding and decoding for synthetic generation.
 */
public class TelemetryRecord implements Serializable {
    private static final long serialVersionUID = 1L;

    // Protobuf Field Tags
    private static final int FIELD_SENSOR_ID = 1;
    private static final int FIELD_TIMESTAMP_MS = 2;
    private static final int FIELD_VALUE = 3;

    private final int sensorId;
    private final long timestampMs;
    private final double value;

    public TelemetryRecord(int sensorId, long timestampMs, double value) {
        this.sensorId = sensorId;
        this.timestampMs = timestampMs;
        this.value = value;
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

    /**
     * Serializes this record into Protobuf wire format.
     */
    public byte[] toProtobufBytes() {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            CodedOutputStream output = CodedOutputStream.newInstance(baos);
            output.writeUInt32(FIELD_SENSOR_ID, sensorId);
            output.writeInt64(FIELD_TIMESTAMP_MS, timestampMs);
            output.writeDouble(FIELD_VALUE, value);
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

            while (!input.isAtEnd()) {
                int tag = input.readTag();
                if (tag == 0) break;
                int fieldNumber = tag >>> 3;
                switch (fieldNumber) {
                    case FIELD_SENSOR_ID -> sensorId = input.readUInt32();
                    case FIELD_TIMESTAMP_MS -> timestampMs = input.readInt64();
                    case FIELD_VALUE -> value = input.readDouble();
                    default -> input.skipField(tag);
                }
            }
            return new TelemetryRecord(sensorId, timestampMs, value);
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
                '}';
    }
}
