package com.telemetry.pipeline;

import java.io.Serializable;
import java.util.Objects;

/**
 * 1-hour 8-scalar extrema rollup entity matching ClickHouse iot.sensor_rollup_1h.
 */
public class SensorRollup1h implements Serializable {
    private static final long serialVersionUID = 1L;

    private final int sensorId;
    private final long bucketTs; // epoch seconds aligned to 3600s
    private final long firstTs;  // epoch milliseconds
    private final double firstVal;
    private final long minValTs;  // epoch milliseconds
    private final double minVal;
    private final long maxValTs;  // epoch milliseconds
    private final double maxVal;
    private final long lastTs;   // epoch milliseconds
    private final double lastVal;

    public SensorRollup1h(
            int sensorId,
            long bucketTs,
            long firstTs,
            double firstVal,
            long minValTs,
            double minVal,
            long maxValTs,
            double maxVal,
            long lastTs,
            double lastVal
    ) {
        this.sensorId = sensorId;
        this.bucketTs = bucketTs;
        this.firstTs = firstTs;
        this.firstVal = firstVal;
        this.minValTs = minValTs;
        this.minVal = minVal;
        this.maxValTs = maxValTs;
        this.maxVal = maxVal;
        this.lastTs = lastTs;
        this.lastVal = lastVal;
    }

    public int getSensorId() {
        return sensorId;
    }

    public long getBucketTs() {
        return bucketTs;
    }

    public long getFirstTs() {
        return firstTs;
    }

    public double getFirstVal() {
        return firstVal;
    }

    public long getMinValTs() {
        return minValTs;
    }

    public double getMinVal() {
        return minVal;
    }

    public long getMaxValTs() {
        return maxValTs;
    }

    public double getMaxVal() {
        return maxVal;
    }

    public long getLastTs() {
        return lastTs;
    }

    public double getLastVal() {
        return lastVal;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SensorRollup1h that)) return false;
        return sensorId == that.sensorId &&
                bucketTs == that.bucketTs &&
                firstTs == that.firstTs &&
                Double.compare(that.firstVal, firstVal) == 0 &&
                minValTs == that.minValTs &&
                Double.compare(that.minVal, minVal) == 0 &&
                maxValTs == that.maxValTs &&
                Double.compare(that.maxVal, maxVal) == 0 &&
                lastTs == that.lastTs &&
                Double.compare(that.lastVal, lastVal) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(sensorId, bucketTs, firstTs, firstVal, minValTs, minVal, maxValTs, maxVal, lastTs, lastVal);
    }

    @Override
    public String toString() {
        return "SensorRollup1h{" +
                "sensorId=" + sensorId +
                ", bucketTs=" + bucketTs +
                ", firstTs=" + firstTs +
                ", firstVal=" + firstVal +
                ", minValTs=" + minValTs +
                ", minVal=" + minVal +
                ", maxValTs=" + maxValTs +
                ", maxVal=" + maxVal +
                ", lastTs=" + lastTs +
                ", lastVal=" + lastVal +
                '}';
    }
}
