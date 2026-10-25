package com.imoil.formula.domain;

/**
 * Backward compatibility wrapper for DynamicRule in the com.imoil.formula.domain package.
 */
public class DynamicRule extends com.telemetry.engine.domain.DynamicRule {

    private static final long serialVersionUID = 1L;

    public DynamicRule() {
        super();
    }

    public DynamicRule(String ruleId, String targetSensorId, String expression, boolean usesWindow, String derivedSensorId) {
        super(ruleId, targetSensorId, expression, usesWindow, derivedSensorId);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends com.telemetry.engine.domain.DynamicRule.Builder {
        @Override
        public Builder ruleId(String ruleId) {
            super.ruleId(ruleId);
            return this;
        }

        @Override
        public Builder targetSensorId(String targetSensorId) {
            super.targetSensorId(targetSensorId);
            return this;
        }

        @Override
        public Builder expression(String expression) {
            super.expression(expression);
            return this;
        }

        @Override
        public Builder usesWindow(boolean usesWindow) {
            super.usesWindow(usesWindow);
            return this;
        }

        @Override
        public Builder derivedSensorId(String derivedSensorId) {
            super.derivedSensorId(derivedSensorId);
            return this;
        }

        @Override
        public DynamicRule build() {
            DynamicRule rule = new DynamicRule();
            com.telemetry.engine.domain.DynamicRule parent = super.build();
            rule.setRuleId(parent.getRuleId());
            rule.setTargetSensorId(parent.getTargetSensorId());
            rule.setExpression(parent.getExpression());
            rule.setUsesWindow(parent.isUsesWindow());
            rule.setDerivedSensorId(parent.getDerivedSensorId());
            return rule;
        }
    }
}
