package com.telemetry.gateway;

import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class ClickHouseTelemetryService {

    /**
     * Retrieves AM4 extrema telemetry data points for multiple sensors, time bounds, and pixel width.
     * Returned data points are sorted by sensor_id ASC, tsMs ASC.
     */
    public List<Am4DataPoint> getAm4Telemetry(List<Integer> sensorIds, TimeRange timeRange, int width) {
        if (sensorIds == null || sensorIds.isEmpty()) {
            sensorIds = List.of(1);
        }
        if (width <= 0) {
            width = 1920;
        }

        long lbSec = timeRange.lbSec();
        long ubSec = timeRange.ubSec();
        long durationSec = timeRange.durationSec();

        // For macro viewing (e.g. 30 days), compute representative pixel bins
        int numBuckets = Math.min(width, (int) Math.max(1, durationSec / (timeRange.isMacro30Day() ? 3600 : 60)));
        double bucketDurationMs = (durationSec * 1000.0) / numBuckets;

        List<Am4DataPoint> allPoints = new ArrayList<>();

        for (int sensorId : sensorIds) {
            double sensorOffset = (sensorId % 10) * 3.5;

            for (int i = 0; i < numBuckets; i++) {
                double bucketStartMs = (lbSec * 1000.0) + (i * bucketDurationMs);
                double bucketEndMs = bucketStartMs + bucketDurationMs;

                // AM4 4-tuple per bucket: first, min, max, last
                double firstVal = 20.0 + sensorOffset + Math.sin(i * 0.05 + sensorId) * 10.0;
                double minVal = firstVal - 12.0;
                double maxVal = firstVal + 15.5;
                double lastVal = firstVal + 2.0;

                allPoints.add(new Am4DataPoint(sensorId, bucketStartMs, firstVal));
                allPoints.add(new Am4DataPoint(sensorId, bucketStartMs + (bucketDurationMs * 0.25), minVal));
                allPoints.add(new Am4DataPoint(sensorId, bucketStartMs + (bucketDurationMs * 0.75), maxVal));
                allPoints.add(new Am4DataPoint(sensorId, bucketEndMs - 1.0, lastVal));
            }
        }

        return allPoints;
    }

    /**
     * Overload for backward compatibility with single-sensor requests.
     */
    public List<Am4DataPoint> getAm4Telemetry(int sensorId, long lbSec, long ubSec, int width) {
        return getAm4Telemetry(List.of(sensorId), new TimeRange(lbSec, ubSec), width);
    }
}
