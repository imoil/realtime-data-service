package com.telemetry.pipeline;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;

import java.io.IOException;
import java.io.Serializable;
import java.util.Objects;

/**
 * Single-sensor measurement item within a batched device telemetry payload.
 */
public class DeviceSensorMetric implements Serializable {
    private static final long serialVersionUID = 1L;

    public static final int FIELD_SENSOR_INDEX = 1;
    public static final int FIELD_VALUE = 2;
    public static final int FIELD_STATE = 3;

    private final int sensorIndex;
    private final double value;
    private final int state;

    public DeviceSensorMetric(int sensorIndex, double value) {
        this(sensorIndex, value, 0);
    }

    public DeviceSensorMetric(int sensorIndex, double value, int state) {
        this.sensorIndex = sensorIndex;
        this.value = value;
        this.state = state;
    }

    public int getSensorIndex() {
        return sensorIndex;
    }

    public double getValue() {
        return value;
    }

    public int getState() {
        return state;
    }

    public int computeSerializedSize() {
        int size = 0;
        size += CodedOutputStream.computeInt32Size(FIELD_SENSOR_INDEX, sensorIndex);
        size += CodedOutputStream.computeDoubleSize(FIELD_VALUE, value);
        if (state != 0) {
            size += CodedOutputStream.computeUInt32Size(FIELD_STATE, state);
        }
        return size;
    }

    public void writeTo(CodedOutputStream output) throws IOException {
        output.writeInt32(FIELD_SENSOR_INDEX, sensorIndex);
        output.writeDouble(FIELD_VALUE, value);
        if (state != 0) {
            output.writeUInt32(FIELD_STATE, state);
        }
    }

    public static DeviceSensorMetric parseFrom(CodedInputStream input) throws IOException {
        int sensorIndex = 0;
        double value = 0.0;
        int state = 0;

        while (!input.isAtEnd()) {
            int tag = input.readTag();
            if (tag == 0) break;
            int fieldNumber = tag >>> 3;
            switch (fieldNumber) {
                case FIELD_SENSOR_INDEX -> sensorIndex = input.readInt32();
                case FIELD_VALUE -> value = input.readDouble();
                case FIELD_STATE -> state = input.readUInt32();
                default -> input.skipField(tag);
            }
        }
        return new DeviceSensorMetric(sensorIndex, value, state);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DeviceSensorMetric that)) return false;
        return sensorIndex == that.sensorIndex &&
                Double.compare(that.value, value) == 0 &&
                state == that.state;
    }

    @Override
    public int hashCode() {
        return Objects.hash(sensorIndex, value, state);
    }

    @Override
    public String toString() {
        return "DeviceSensorMetric{" +
                "sensorIndex=" + sensorIndex +
                ", value=" + value +
                ", state=" + state +
                '}';
    }
}
