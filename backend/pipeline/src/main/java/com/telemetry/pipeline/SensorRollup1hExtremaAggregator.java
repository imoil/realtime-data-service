package com.telemetry.pipeline;

import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * Flink AggregateFunction computing 1-hour 8-scalar extrema.
 */
public class SensorRollup1hExtremaAggregator implements AggregateFunction<TelemetryRecord, ExtremaAccumulator, SensorRollup1h> {
    private static final long serialVersionUID = 1L;

    @Override
    public ExtremaAccumulator createAccumulator() {
        return new ExtremaAccumulator();
    }

    @Override
    public ExtremaAccumulator add(TelemetryRecord record, ExtremaAccumulator accumulator) {
        accumulator.add(record);
        return accumulator;
    }

    @Override
    public SensorRollup1h getResult(ExtremaAccumulator accumulator) {
        if (accumulator.count == 0) {
            return null;
        }
        // bucketTs is epoch seconds aligned to 1-hour (3600-second) window
        long bucketTs = (accumulator.firstTs / 3600000L) * 3600L;
        return new SensorRollup1h(
                accumulator.sensorId,
                bucketTs,
                accumulator.firstTs,
                accumulator.firstVal,
                accumulator.minValTs,
                accumulator.minVal,
                accumulator.maxValTs,
                accumulator.maxVal,
                accumulator.lastTs,
                accumulator.lastVal
        );
    }

    @Override
    public ExtremaAccumulator merge(ExtremaAccumulator a, ExtremaAccumulator b) {
        a.merge(b);
        return a;
    }
}
