package com.telemetry.engine.window;

import com.telemetry.engine.RuleEngineConfig;
import com.telemetry.engine.RuleEngineService;
import com.telemetry.engine.functions.WindowAvgFunction;
import com.telemetry.engine.functions.WindowMaxFunction;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CompactCircularDoubleBufferTest {

    @Test
    @DisplayName("Invalid buffer capacity throws IllegalArgumentException")
    void testInvalidCapacity() {
        assertThatThrownBy(() -> new CompactCircularDoubleBuffer(0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new CompactCircularDoubleBuffer(32768))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("30-minute sliding window (1800 ticks) executes cleanly with zero-allocation view")
    void testThirtyMinuteSlidingWindow() {
        final int thirtyMinuteTicks = 1800;
        CompactCircularDoubleBuffer buffer = new CompactCircularDoubleBuffer(thirtyMinuteTicks);

        long baseTimestamp = 1729800000000L;
        double sum = 0.0;
        double max = -Double.MAX_VALUE;

        for (int i = 1; i <= thirtyMinuteTicks; i++) {
            double value = (double) i;
            buffer.add(value, baseTimestamp + i * 1000L);
            sum += value;
            if (value > max) {
                max = value;
            }
        }

        assertThat(buffer.getCount()).isEqualTo(thirtyMinuteTicks);
        assertThat(buffer.getCapacity()).isEqualTo(thirtyMinuteTicks);
        assertThat(buffer.getLastTimestampMs()).isEqualTo(baseTimestamp + thirtyMinuteTicks * 1000L);

        double expectedAvg = sum / thirtyMinuteTicks;
        assertThat(buffer.computeAverage(thirtyMinuteTicks)).isEqualTo(expectedAvg);
        assertThat(buffer.computeMax(thirtyMinuteTicks)).isEqualTo(max);

        List<Double> listView = buffer.asListView(thirtyMinuteTicks);
        assertThat(listView).hasSize(thirtyMinuteTicks);
        assertThat(listView.get(0)).isEqualTo(1.0);
        assertThat(listView.get(thirtyMinuteTicks - 1)).isEqualTo(1800.0);

        RuleEngineService service = new RuleEngineService(
                RuleEngineConfig.createEvaluatorInstance(new WindowAvgFunction(), new WindowMaxFunction())
        );

        Map<String, Object> env = new HashMap<>();
        env.put("value", 50.0);
        env.put("window_data", listView);

        double evaluatedAvg = service.evaluate("window_avg(1800)", env);
        assertThat(evaluatedAvg).isEqualTo(expectedAvg);

        double evaluatedMax = service.evaluate("window_max(1800)", env);
        assertThat(evaluatedMax).isEqualTo(max);

        double combinedResult = service.evaluate("value * 2.0 + window_avg(10)", env);
        assertThat(combinedResult).isEqualTo(1895.5);
    }

    @Test
    @DisplayName("Circular buffer rollover evicts oldest items correctly")
    void testRolloverEviction() {
        int capacity = 5;
        CompactCircularDoubleBuffer buffer = new CompactCircularDoubleBuffer(capacity);

        for (int i = 1; i <= 7; i++) {
            buffer.add((double) i, i * 1000L);
        }

        assertThat(buffer.getCount()).isEqualTo(5);
        List<Double> view = buffer.asListView(5);
        assertThat(view).containsExactly(3.0, 4.0, 5.0, 6.0, 7.0);

        List<Double> partialView = buffer.asListView(3);
        assertThat(partialView).containsExactly(5.0, 6.0, 7.0);
    }

    @Test
    @DisplayName("State serialization to and from bytes preserves complete buffer state")
    void testByteSerialization() {
        CompactCircularDoubleBuffer buffer = new CompactCircularDoubleBuffer(10);
        for (int i = 1; i <= 5; i++) {
            buffer.add(i * 10.0, 1000L * i);
        }

        byte[] serialized = buffer.toBytes();
        CompactCircularDoubleBuffer restored = CompactCircularDoubleBuffer.fromBytes(serialized);

        assertThat(restored).isNotNull();
        assertThat(restored.getCount()).isEqualTo(5);
        assertThat(restored.getCapacity()).isEqualTo(10);
        assertThat(restored.getLastTimestampMs()).isEqualTo(5000L);
        assertThat(restored.asListView(5)).containsExactly(10.0, 20.0, 30.0, 40.0, 50.0);
    }
}
