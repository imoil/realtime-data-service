package com.imoil.formula.engine;

import com.googlecode.aviator.AviatorEvaluatorInstance;

import java.util.Map;

/**
 * Backward compatibility wrapper for RuleEngineService in com.imoil.formula.engine.
 */
public class RuleEngineService extends com.telemetry.engine.RuleEngineService {

    public RuleEngineService() {
        super();
    }

    public RuleEngineService(AviatorEvaluatorInstance evaluator) {
        super(evaluator);
    }
}
