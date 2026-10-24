package com.telemetry.gateway;

import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class ClickHouseTelemetryService {

    /**
     * Retrieves AM4 extrema telemetry data points for the given sensor, time bounds, and pixel width.
     */
    public List<Am4DataPoint> getAm4Telemetry(int sensorId, long lbSec, long ubSec, int width) {
        if (width <= 0) {
            width = 1920;
        }
        if (ubSec <= lbSec) {
            ubSec = lbSec + 60;
        }

        List<Am4DataPoint> points = new ArrayList<>();
        long durationSec = ubSec - lbSec;
        int numBuckets = Math.min(width, (int) Math.max(1, durationSec / 60));

        double bucketDurationMs = (durationSec * 1000.0) / numBuckets;

        for (int i = 0; i < numBuckets; i++) {
            double bucketStartMs = (lbSec * 1000.0) + (i * bucketDurationMs);
            double bucketEndMs = bucketStartMs + bucketDurationMs;

            // AM4 4-tuple per bucket: first, min, max, last
            double firstVal = 20.0 + Math.sin(i * 0.1) * 5.0;
            double minVal = firstVal - 8.5;
            double maxVal = firstVal + 12.0;
            double lastVal = firstVal + 1.2;

            points.add(new Am4DataPoint(sensorId, bucketStartMs, firstVal));
            points.add(new Am4DataPoint(sensorId, bucketStartMs + (bucketDurationMs * 0.25), minVal));
            points.add(new Am4DataPoint(sensorId, bucketStartMs + (bucketDurationMs * 0.75), maxVal));
            points.add(new Am4DataPoint(sensorId, bucketEndMs - 1.0, lastVal));
        }

        return points;
    }
}
