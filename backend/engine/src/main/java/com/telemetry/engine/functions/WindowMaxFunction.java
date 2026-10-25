package com.telemetry.engine.functions;

import java.util.List;

/**
 * Custom AviatorScript window function: window_max(size).
 * Computes maximum value of the most recent N window elements.
 */
public class WindowMaxFunction extends AbstractWindowFunction {

    @Override
    public String getName() {
        return "window_max";
    }

    @Override
    protected double compute(List<Double> subList) {
        if (subList.isEmpty()) {
            return -Double.MAX_VALUE;
        }
        double max = -Double.MAX_VALUE;
        for (Double value : subList) {
            if (value > max) {
                max = value;
            }
        }
        return max;
    }
}
