package com.telemetry.pipeline;

import java.io.Serializable;

/**
 * Accumulator tracking 8-scalar extrema during window aggregation.
 */
public class ExtremaAccumulator implements Serializable {
    private static final long serialVersionUID = 1L;

    public int sensorId = -1;
    public long count = 0;

    public long firstTs = Long.MAX_VALUE;
    public double firstVal = 0.0;

    public long minValTs = 0L;
    public double minVal = Double.POSITIVE_INFINITY;

    public long maxValTs = 0L;
    public double maxVal = Double.NEGATIVE_INFINITY;

    public long lastTs = Long.MIN_VALUE;
    public double lastVal = 0.0;

    public void add(TelemetryRecord record) {
        if (count == 0) {
            sensorId = record.getSensorId();
        }
        count++;

        long ts = record.getTimestampMs();
        double val = record.getValue();

        // First point by timestamp
        if (ts < firstTs) {
            firstTs = ts;
            firstVal = val;
        }

        // Min point by value
        if (val < minVal) {
            minVal = val;
            minValTs = ts;
        }

        // Max point by value
        if (val > maxVal) {
            maxVal = val;
            maxValTs = ts;
        }

        // Last point by timestamp
        if (ts >= lastTs) {
            lastTs = ts;
            lastVal = val;
        }
    }

    public void merge(ExtremaAccumulator other) {
        if (other.count == 0) return;
        if (this.count == 0) {
            this.sensorId = other.sensorId;
            this.count = other.count;
            this.firstTs = other.firstTs;
            this.firstVal = other.firstVal;
            this.minValTs = other.minValTs;
            this.minVal = other.minVal;
            this.maxValTs = other.maxValTs;
            this.maxVal = other.maxVal;
            this.lastTs = other.lastTs;
            this.lastVal = other.lastVal;
            return;
        }

        this.count += other.count;

        if (other.firstTs < this.firstTs) {
            this.firstTs = other.firstTs;
            this.firstVal = other.firstVal;
        }

        if (other.minVal < this.minVal) {
            this.minVal = other.minVal;
            this.minValTs = other.minValTs;
        }

        if (other.maxVal > this.maxVal) {
            this.maxVal = other.maxVal;
            this.maxValTs = other.maxValTs;
        }

        if (other.lastTs >= this.lastTs) {
            this.lastTs = other.lastTs;
            this.lastVal = other.lastVal;
        }
    }
}
