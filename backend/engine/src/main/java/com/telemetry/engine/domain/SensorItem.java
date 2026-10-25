package com.telemetry.engine.domain;

import java.io.Serializable;
import java.util.Objects;

/**
 * Metric measurement reading for an individual sensor.
 */
public class SensorItem implements Serializable {

    private static final long serialVersionUID = 1L;

    private int sensorIndex;
    private double value;
    private String status;

    public SensorItem() {
    }

    public SensorItem(int sensorIndex, double value, String status) {
        this.sensorIndex = sensorIndex;
        this.value = value;
        this.status = status;
    }

    public static Builder builder() {
        return new Builder();
    }

    public int getSensorIndex() {
        return sensorIndex;
    }

    public void setSensorIndex(int sensorIndex) {
        this.sensorIndex = sensorIndex;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SensorItem that)) return false;
        return sensorIndex == that.sensorIndex &&
                Double.compare(that.value, value) == 0 &&
                Objects.equals(status, that.status);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sensorIndex, value, status);
    }

    @Override
    public String toString() {
        return "SensorItem{" +
                "sensorIndex=" + sensorIndex +
                ", value=" + value +
                ", status='" + status + '\'' +
                '}';
    }

    public static class Builder {
        private int sensorIndex;
        private double value;
        private String status;

        public Builder sensorIndex(int sensorIndex) {
            this.sensorIndex = sensorIndex;
            return this;
        }

        public Builder value(double value) {
            this.value = value;
            return this;
        }

        public Builder status(String status) {
            this.status = status;
            return this;
        }

        public SensorItem build() {
            return new SensorItem(sensorIndex, value, status);
        }
    }
}
