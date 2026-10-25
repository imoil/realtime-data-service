package com.imoil.formula.engine;

/**
 * Backward compatibility wrapper for ExpressionValidator in the com.imoil.formula.engine package.
 */
public final class ExpressionValidator {

    private ExpressionValidator() {
    }

    public static void validate(String expressionText) {
        com.telemetry.engine.ExpressionValidator.validate(expressionText);
    }
}
