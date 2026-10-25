package com.telemetry.engine.functions;

import com.googlecode.aviator.runtime.function.AbstractFunction;
import com.googlecode.aviator.runtime.type.AviatorDouble;
import com.googlecode.aviator.runtime.type.AviatorObject;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Base abstract function for sliding window aggregations over historical telemetry.
 */
public abstract class AbstractWindowFunction extends AbstractFunction {

    @Override
    public AviatorObject call(Map<String, Object> env, AviatorObject arg1) {
        int size = ((Number) arg1.getValue(env)).intValue();

        @SuppressWarnings("unchecked")
        List<Double> windowData = (List<Double>) env.get("window_data");

        if (windowData == null || windowData.isEmpty()) {
            return AviatorDouble.valueOf(0.0);
        }

        if (size <= 0) {
            return AviatorDouble.valueOf(compute(Collections.emptyList()));
        }

        int elements = Math.min(size, windowData.size());
        int startIndex = windowData.size() - elements;
        List<Double> subList = windowData.subList(startIndex, windowData.size());

        return AviatorDouble.valueOf(compute(subList));
    }

    /**
     * Computes the aggregated scalar value over the window slice.
     *
     * @param subList recent telemetry values in chronological order
     * @return computed aggregation result
     */
    protected abstract double compute(List<Double> subList);
}
