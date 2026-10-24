package com.telemetry.pipeline;

import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * Flink AggregateFunction computing 1-minute 8-scalar extrema.
 */
public class SensorRollupExtremaAggregator implements AggregateFunction<TelemetryRecord, ExtremaAccumulator, SensorRollup1m> {
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
    public SensorRollup1m getResult(ExtremaAccumulator accumulator) {
        if (accumulator.count == 0) {
            return null;
        }
        // bucketTs is epoch seconds aligned to 60-second window
        long bucketTs = (accumulator.firstTs / 60000L) * 60L;
        return new SensorRollup1m(
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
