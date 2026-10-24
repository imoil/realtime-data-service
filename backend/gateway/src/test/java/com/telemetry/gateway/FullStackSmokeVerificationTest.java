package com.telemetry.gateway;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.asyncDispatch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.request;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Automated end-to-end smoke verification test ensuring all infrastructure
 * bindings and gateway components operate harmoniously.
 */
@SpringBootTest
@AutoConfigureMockMvc
class FullStackSmokeVerificationTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private TelemetryQueryRouter queryRouter;

    @Autowired
    private LiveTelemetrySubscriptionRegistry subscriptionRegistry;

    @Autowired
    private KafkaLiveTelemetryBridgeService bridgeService;

    @Test
    @DisplayName("Smoke Test 1: HTTP GET /api/v1/telemetry/am4-arrow returns binary Arrow stream")
    void testArrowStreamEndpointReturnsBinaryContent() throws Exception {
        MvcResult asyncResult = mockMvc.perform(get("/api/v1/telemetry/am4-arrow")
                        .param("sensor_id", "1")
                        .param("lb_sec", "1729728000")
                        .param("ub_sec", "1729728060")
                        .param("width", "1920"))
                .andExpect(request().asyncStarted())
                .andReturn();

        MvcResult result = mockMvc.perform(asyncDispatch(asyncResult))
                .andExpect(status().isOk())
                .andExpect(header().string("Content-Type", "application/vnd.apache.arrow.stream"))
                .andReturn();

        byte[] body = result.getResponse().getContentAsByteArray();
        assertThat(body).isNotNull();
        assertThat(body.length).isGreaterThan(0);
    }

    @Test
    @DisplayName("Smoke Test 2: TelemetryQueryRouter produces valid ClickHouse DDL & queries for all resolutions")
    void testQueryRouterGeneratesValidSql() {
        // Intermediate range (1 day) -> sensor_rollup_1m
        TimeRange intermediateRange = new TimeRange(1700000000L, 1700000000L + 86400L);
        QueryRoutingDecision d1m = queryRouter.resolve(intermediateRange, List.of(1), 1920);
        assertThat(d1m.targetTable()).isEqualTo("iot.sensor_rollup_1m");
        assertThat(d1m.sqlQuery()).contains("FROM iot.sensor_rollup_1m");
        assertThat(d1m.sqlQuery()).contains("FORMAT ArrowStream");

        // Macro range (14 days) -> sensor_rollup_1h
        TimeRange macroRange = new TimeRange(1700000000L, 1700000000L + 14 * 86400L);
        QueryRoutingDecision d1h = queryRouter.resolve(macroRange, List.of(2), 1920);
        assertThat(d1h.targetTable()).isEqualTo("iot.sensor_rollup_1h");
        assertThat(d1h.sqlQuery()).contains("FROM iot.sensor_rollup_1h");
        assertThat(d1h.sqlQuery()).contains("FORMAT ArrowStream");

        // Micro-zoom range (100 seconds) -> sensor_iceberg_raw
        TimeRange microRange = new TimeRange(1700000000L, 1700000000L + 100L);
        QueryRoutingDecision dRaw = queryRouter.resolve(microRange, List.of(3), 1920);
        assertThat(dRaw.targetTable()).isEqualTo("iot.sensor_iceberg_raw");
        assertThat(dRaw.sqlQuery()).contains("FROM iot.sensor_iceberg_raw");
        assertThat(dRaw.sqlQuery()).contains("FORMAT ArrowStream");
    }

    @Test
    @DisplayName("Smoke Test 3: Dynamic WebSocket tail routing filters and forwards live ticks")
    void testWebSocketLiveTailPipeline() throws Exception {
        List<String> messages = Collections.synchronizedList(new ArrayList<>());
        KafkaLiveTelemetryBridgeTest.DummyWebSocketSession session =
                new KafkaLiveTelemetryBridgeTest.DummyWebSocketSession("smoke-client-1", messages);

        try {
            subscriptionRegistry.subscribe(session, List.of(101));

            // Produce tick for sensor 101 (subscribed)
            bridgeService.processTick(new TelemetryTick(101, 1729728000000L, 99.9));

            // Produce tick for sensor 202 (not subscribed)
            bridgeService.processTick(new TelemetryTick(202, 1729728000000L, 11.1));

            Thread.sleep(60);

            assertThat(messages).hasSize(1);
            assertThat(messages.get(0)).contains("\"sensorId\":101");
            assertThat(messages.get(0)).contains("\"value\":99.9000");
        } finally {
            subscriptionRegistry.removeSession("smoke-client-1");
        }
    }
}
