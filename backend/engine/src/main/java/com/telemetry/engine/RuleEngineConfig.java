package com.telemetry.engine;

import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.AviatorEvaluatorInstance;
import com.googlecode.aviator.Feature;
import com.googlecode.aviator.Options;
import com.telemetry.engine.functions.WindowAvgFunction;
import com.telemetry.engine.functions.WindowMaxFunction;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

/**
 * Factory for creating hardened, sandboxed AviatorScript evaluator instances.
 * Pure Java configuration with no Spring framework dependencies.
 */
public final class RuleEngineConfig {

    private RuleEngineConfig() {
    }

    /**
     * Creates a hardened AviatorEvaluatorInstance with default window functions registered.
     */
    public static AviatorEvaluatorInstance createEvaluatorInstance() {
        return createEvaluatorInstance(new WindowAvgFunction(), new WindowMaxFunction());
    }

    /**
     * Creates a hardened AviatorEvaluatorInstance with specified window functions.
     *
     * @param windowAvgFunction window_avg function implementation
     * @param windowMaxFunction window_max function implementation
     * @return secure AviatorEvaluatorInstance
     */
    public static AviatorEvaluatorInstance createEvaluatorInstance(
            WindowAvgFunction windowAvgFunction,
            WindowMaxFunction windowMaxFunction) {

        AviatorEvaluatorInstance instance = AviatorEvaluator.newInstance();
        instance.setCachedExpressionByDefault(true);

        // Security Sandbox: deny reflection and class access
        instance.setOption(Options.ALLOWED_CLASS_SET, Collections.emptySet());

        // Disable unsafe language features
        Set<Feature> safeFeatures = EnumSet.allOf(Feature.class);
        safeFeatures.remove(Feature.NewInstance);
        safeFeatures.remove(Feature.StaticMethods);
        safeFeatures.remove(Feature.StaticFields);
        safeFeatures.remove(Feature.Use);
        safeFeatures.remove(Feature.Module);
        safeFeatures.remove(Feature.ExceptionHandle);
        safeFeatures.remove(Feature.Assignment);
        safeFeatures.remove(Feature.Let);
        safeFeatures.remove(Feature.Lambda);
        safeFeatures.remove(Feature.Fn);
        safeFeatures.remove(Feature.ForLoop);
        safeFeatures.remove(Feature.WhileLoop);
        safeFeatures.remove(Feature.Return);
        safeFeatures.remove(Feature.StringInterpolation);
        instance.setOption(Options.FEATURE_SET, safeFeatures);

        // Protect against DoS
        instance.setOption(Options.MAX_LOOP_COUNT, 1000);
        instance.setOption(Options.EVAL_TIMEOUT_MS, 5000L);

        // Register window aggregation functions
        if (windowAvgFunction != null) {
            instance.addFunction(windowAvgFunction);
        }
        if (windowMaxFunction != null) {
            instance.addFunction(windowMaxFunction);
        }

        return instance;
    }
}
