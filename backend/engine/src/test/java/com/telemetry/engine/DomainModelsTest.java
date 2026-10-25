package com.telemetry.engine;

import com.telemetry.engine.domain.DynamicRule;
import com.telemetry.engine.domain.SensorItem;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import static org.assertj.core.api.Assertions.assertThat;

class DomainModelsTest {

    @Test
    @DisplayName("DynamicRule builder, getters, and serialization operate correctly")
    void testDynamicRule() throws Exception {
        DynamicRule rule = DynamicRule.builder()
                .ruleId("RULE-001")
                .targetSensorId("10001")
                .expression("value * 1.5 + window_avg(5)")
                .usesWindow(true)
                .derivedSensorId("20001")
                .build();

        assertThat(rule.getRuleId()).isEqualTo("RULE-001");
        assertThat(rule.getTargetSensorId()).isEqualTo("10001");
        assertThat(rule.getExpression()).isEqualTo("value * 1.5 + window_avg(5)");
        assertThat(rule.isUsesWindow()).isTrue();
        assertThat(rule.getDerivedSensorId()).isEqualTo("20001");

        DynamicRule rule2 = new DynamicRule("RULE-001", "10001", "value * 1.5 + window_avg(5)", true, "20001");
        assertThat(rule).isEqualTo(rule2);
        assertThat(rule.hashCode()).isEqualTo(rule2.hashCode());
        assertThat(rule.toString()).contains("RULE-001");

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(rule);
        }

        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()))) {
            DynamicRule deserialized = (DynamicRule) ois.readObject();
            assertThat(deserialized).isEqualTo(rule);
        }
    }

    @Test
    @DisplayName("SensorItem builder, getters, and equality operate correctly")
    void testSensorItem() throws Exception {
        SensorItem item = SensorItem.builder()
                .sensorIndex(3)
                .value(42.5)
                .status("normal")
                .build();

        assertThat(item.getSensorIndex()).isEqualTo(3);
        assertThat(item.getValue()).isEqualTo(42.5);
        assertThat(item.getStatus()).isEqualTo("normal");

        SensorItem item2 = new SensorItem(3, 42.5, "normal");
        assertThat(item).isEqualTo(item2);
        assertThat(item.hashCode()).isEqualTo(item2.hashCode());
        assertThat(item.toString()).contains("42.5");

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(item);
        }

        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()))) {
            SensorItem deserialized = (SensorItem) ois.readObject();
            assertThat(deserialized).isEqualTo(item);
        }
    }

    @Test
    @DisplayName("Backward compatibility package classes instantiate correctly")
    void testBackwardCompatibilityModels() {
        com.imoil.formula.domain.DynamicRule legacyRule = com.imoil.formula.domain.DynamicRule.builder()
                .ruleId("R1")
                .targetSensorId("101")
                .expression("value * 2")
                .usesWindow(false)
                .derivedSensorId("201")
                .build();
        assertThat(legacyRule.getRuleId()).isEqualTo("R1");

        com.imoil.formula.domain.SensorItem legacyItem = com.imoil.formula.domain.SensorItem.builder()
                .sensorIndex(1)
                .value(10.0)
                .status("normal")
                .build();
        assertThat(legacyItem.getSensorIndex()).isEqualTo(1);
    }
}
