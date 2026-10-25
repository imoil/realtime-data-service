package com.imoil.formula.engine;

import com.googlecode.aviator.AviatorEvaluatorInstance;
import com.telemetry.engine.functions.WindowAvgFunction;
import com.telemetry.engine.functions.WindowMaxFunction;

/**
 * Backward compatibility wrapper for RuleEngineConfig in com.imoil.formula.engine.
 */
public final class RuleEngineConfig {

    public AviatorEvaluatorInstance aviatorEvaluatorInstance(
            WindowAvgFunction windowAvgFunction,
            WindowMaxFunction windowMaxFunction) {
        return com.telemetry.engine.RuleEngineConfig.createEvaluatorInstance(windowAvgFunction, windowMaxFunction);
    }
}
