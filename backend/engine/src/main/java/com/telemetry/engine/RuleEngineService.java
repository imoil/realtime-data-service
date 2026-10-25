package com.telemetry.engine;

import com.googlecode.aviator.AviatorEvaluatorInstance;
import com.googlecode.aviator.Expression;

import java.util.Map;
import java.util.Objects;

/**
 * High-performance formula evaluation service utilizing sandboxed AviatorScript JIT compiler.
 * Thread-safe and independent of Spring framework.
 */
public class RuleEngineService {

    private final AviatorEvaluatorInstance evaluator;

    public RuleEngineService() {
        this(RuleEngineConfig.createEvaluatorInstance());
    }

    public RuleEngineService(AviatorEvaluatorInstance evaluator) {
        this.evaluator = Objects.requireNonNull(evaluator, "evaluator must not be null");
    }

    /**
     * Validates and evaluates expression text with variable environment.
     * Uses LRU JIT bytecode caching.
     *
     * @param expressionText mathematical or logical formula
     * @param env variable bindings including 'value' and optional 'window_data'
     * @return numeric result as double
     * @throws IllegalArgumentException if expression fails security check or does not evaluate to number
     */
    public double evaluate(String expressionText, Map<String, Object> env) {
        ExpressionValidator.validate(expressionText);
        Expression expression = evaluator.compile(expressionText, true);
        Object result = expression.execute(env);

        if (result instanceof Number num) {
            return num.doubleValue();
        }

        throw new IllegalArgumentException("The evaluation result could not be cast to a numeric value.");
    }

    public AviatorEvaluatorInstance getEvaluator() {
        return evaluator;
    }
}
