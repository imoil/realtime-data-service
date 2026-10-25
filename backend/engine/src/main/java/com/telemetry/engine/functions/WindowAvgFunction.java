package com.telemetry.engine.functions;

import java.util.List;

/**
 * Custom AviatorScript window function: window_avg(size).
 * Computes arithmetic mean of the most recent N window elements.
 */
public class WindowAvgFunction extends AbstractWindowFunction {

    @Override
    public String getName() {
        return "window_avg";
    }

    @Override
    protected double compute(List<Double> subList) {
        if (subList.isEmpty()) {
            return 0.0;
        }
        double sum = 0.0;
        for (Double value : subList) {
            sum += value;
        }
        return sum / subList.size();
    }
}
