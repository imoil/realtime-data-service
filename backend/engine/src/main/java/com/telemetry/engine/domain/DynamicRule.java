package com.telemetry.engine.domain;

import java.io.Serializable;
import java.util.Objects;

/**
 * Dynamic rule metadata model for runtime expression evaluation.
 */
public class DynamicRule implements Serializable {

    private static final long serialVersionUID = 1L;

    private String ruleId;
    private String targetSensorId;
    private String expression;
    private boolean usesWindow;
    private String derivedSensorId;

    public DynamicRule() {
    }

    public DynamicRule(String ruleId, String targetSensorId, String expression, boolean usesWindow, String derivedSensorId) {
        this.ruleId = ruleId;
        this.targetSensorId = targetSensorId;
        this.expression = expression;
        this.usesWindow = usesWindow;
        this.derivedSensorId = derivedSensorId;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getRuleId() {
        return ruleId;
    }

    public void setRuleId(String ruleId) {
        this.ruleId = ruleId;
    }

    public String getTargetSensorId() {
        return targetSensorId;
    }

    public void setTargetSensorId(String targetSensorId) {
        this.targetSensorId = targetSensorId;
    }

    public String getExpression() {
        return expression;
    }

    public void setExpression(String expression) {
        this.expression = expression;
    }

    public boolean isUsesWindow() {
        return usesWindow;
    }

    public void setUsesWindow(boolean usesWindow) {
        this.usesWindow = usesWindow;
    }

    public String getDerivedSensorId() {
        return derivedSensorId;
    }

    public void setDerivedSensorId(String derivedSensorId) {
        this.derivedSensorId = derivedSensorId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DynamicRule that)) return false;
        return usesWindow == that.usesWindow &&
                Objects.equals(ruleId, that.ruleId) &&
                Objects.equals(targetSensorId, that.targetSensorId) &&
                Objects.equals(expression, that.expression) &&
                Objects.equals(derivedSensorId, that.derivedSensorId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ruleId, targetSensorId, expression, usesWindow, derivedSensorId);
    }

    @Override
    public String toString() {
        return "DynamicRule{" +
                "ruleId='" + ruleId + '\'' +
                ", targetSensorId='" + targetSensorId + '\'' +
                ", expression='" + expression + '\'' +
                ", usesWindow=" + usesWindow +
                ", derivedSensorId='" + derivedSensorId + '\'' +
                '}';
    }

    public static class Builder {
        private String ruleId;
        private String targetSensorId;
        private String expression;
        private boolean usesWindow;
        private String derivedSensorId;

        public Builder ruleId(String ruleId) {
            this.ruleId = ruleId;
            return this;
        }

        public Builder targetSensorId(String targetSensorId) {
            this.targetSensorId = targetSensorId;
            return this;
        }

        public Builder expression(String expression) {
            this.expression = expression;
            return this;
        }

        public Builder usesWindow(boolean usesWindow) {
            this.usesWindow = usesWindow;
            return this;
        }

        public Builder derivedSensorId(String derivedSensorId) {
            this.derivedSensorId = derivedSensorId;
            return this;
        }

        public DynamicRule build() {
            return new DynamicRule(ruleId, targetSensorId, expression, usesWindow, derivedSensorId);
        }
    }
}
