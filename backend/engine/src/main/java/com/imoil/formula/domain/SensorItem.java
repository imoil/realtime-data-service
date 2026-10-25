package com.imoil.formula.domain;

/**
 * Backward compatibility wrapper for SensorItem in the com.imoil.formula.domain package.
 */
public class SensorItem extends com.telemetry.engine.domain.SensorItem {

    private static final long serialVersionUID = 1L;

    public SensorItem() {
        super();
    }

    public SensorItem(int sensorIndex, double value, String status) {
        super(sensorIndex, value, status);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends com.telemetry.engine.domain.SensorItem.Builder {
        @Override
        public Builder sensorIndex(int sensorIndex) {
            super.sensorIndex(sensorIndex);
            return this;
        }

        @Override
        public Builder value(double value) {
            super.value(value);
            return this;
        }

        @Override
        public Builder status(String status) {
            super.status(status);
            return this;
        }

        @Override
        public SensorItem build() {
            SensorItem item = new SensorItem();
            com.telemetry.engine.domain.SensorItem parent = super.build();
            item.setSensorIndex(parent.getSensorIndex());
            item.setValue(parent.getValue());
            item.setStatus(parent.getStatus());
            return item;
        }
    }
}
