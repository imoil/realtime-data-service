package com.telemetry.engine;

import com.googlecode.aviator.AviatorEvaluatorInstance;
import com.telemetry.engine.functions.WindowAvgFunction;
import com.telemetry.engine.functions.WindowMaxFunction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RuleEngineServiceTest {

    private RuleEngineService ruleEngineService;

    @BeforeEach
    void setUp() {
        AviatorEvaluatorInstance instance = RuleEngineConfig.createEvaluatorInstance(
                new WindowAvgFunction(),
                new WindowMaxFunction()
        );
        ruleEngineService = new RuleEngineService(instance);
    }

    @Test
    @DisplayName("Basic arithmetic expressions calculate accurately")
    void testBasicArithmetic() {
        Map<String, Object> env = new HashMap<>();
        env.put("value", 10.5);
        double result = ruleEngineService.evaluate("value * 2.0 + 10", env);
        assertThat(result).isEqualTo(31.0);
    }

    @Test
    @DisplayName("Ternary expressions branch correctly")
    void testTernaryExpression() {
        Map<String, Object> env = new HashMap<>();
        env.put("value", 60.0);
        double result = ruleEngineService.evaluate("value > 50 ? value * 1.1 : value", env);
        assertThat(result).isEqualTo(66.0);
    }

    @Test
    @DisplayName("Window functions (window_avg, window_max) evaluate accurately with env")
    void testWindowFunctionsWithEnv() {
        Map<String, Object> env = new HashMap<>();
        env.put("value", 10.0);
        env.put("window_data", Arrays.asList(2.0, 4.0, 6.0, 8.0, 10.0));

        double avgResult = ruleEngineService.evaluate("value + window_avg(3)", env);
        assertThat(avgResult).isEqualTo(18.0);

        double maxResult = ruleEngineService.evaluate("window_max(4) * 2", env);
        assertThat(maxResult).isEqualTo(20.0);
    }

    @Test
    @DisplayName("Window functions handle empty or missing window_data safely")
    void testWindowFunctionsWithEmptyEnv() {
        Map<String, Object> env = new HashMap<>();
        double avgResult = ruleEngineService.evaluate("window_avg(3)", env);
        assertThat(avgResult).isEqualTo(0.0);

        double maxResult = ruleEngineService.evaluate("window_max(3)", env);
        assertThat(maxResult).isEqualTo(0.0);
    }

    @Test
    @DisplayName("Null or blank expressions throw IllegalArgumentException")
    void testInvalidExpressionInputs() {
        Map<String, Object> env = new HashMap<>();
        assertThatThrownBy(() -> ruleEngineService.evaluate(null, env))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ruleEngineService.evaluate("", env))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ruleEngineService.evaluate("   ", env))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("Security: Blocks object instantiation (new ...)")
    void testMaliciousNewInstance() {
        Map<String, Object> env = new HashMap<>();
        assertThatThrownBy(() -> ruleEngineService.evaluate("new java.lang.ProcessBuilder('ls').start()", env))
                .isInstanceOf(Exception.class);
    }

    @Test
    @DisplayName("Security: Blocks static methods and reflection")
    void testMaliciousStaticMethodAndReflection() {
        Map<String, Object> env = new HashMap<>();
        assertThatThrownBy(() -> ruleEngineService.evaluate("java.lang.System.exit(0)", env))
                .isInstanceOf(Exception.class);

        assertThatThrownBy(() -> ruleEngineService.evaluate("Class.forName('java.lang.Runtime').getRuntime()", env))
                .isInstanceOf(Exception.class);
    }

    @Test
    @DisplayName("Security: Blocks static field access")
    void testMaliciousStaticFieldAccess() {
        Map<String, Object> env = new HashMap<>();
        assertThatThrownBy(() -> ruleEngineService.evaluate("java.io.File.separator", env))
                .isInstanceOf(Exception.class);
    }

    @Test
    @DisplayName("Security: Blocks use / import statements")
    void testMaliciousUseImport() {
        Map<String, Object> env = new HashMap<>();
        assertThatThrownBy(() -> ruleEngineService.evaluate("use java.lang.System; System.currentTimeMillis()", env))
                .isInstanceOf(Exception.class);
    }

    @Test
    @DisplayName("Security: Blocks infinite loop and while expressions")
    void testLoopCountRestriction() {
        Map<String, Object> env = new HashMap<>();
        assertThatThrownBy(() -> ruleEngineService.evaluate("let i = 0; while(true) { i = i + 1; } return i;", env))
                .isInstanceOf(Exception.class);
    }

    @Test
    @DisplayName("Security: Blocks standalone assignment (=)")
    void testMaliciousAssignment() {
        Map<String, Object> env = new HashMap<>();
        assertThatThrownBy(() -> ruleEngineService.evaluate("a = 10", env))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("Security: Blocks let and lambda syntax")
    void testMaliciousLetAndLambda() {
        Map<String, Object> env = new HashMap<>();
        assertThatThrownBy(() -> ruleEngineService.evaluate("let a = 10", env))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
