package com.telemetry.gateway;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import java.io.ByteArrayInputStream;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.asyncDispatch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.request;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
class MultiSensorMacroRoutingTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private TelemetryQueryRouter queryRouter;

    @Test
    @DisplayName("Routes >7d queries to sensor_rollup_1h and generates single-scan AM4 SQL")
    void testQueryRouterTargets1hForMacroRange() {
        long lbSec = 1729728000L;
        long ubSec30d = lbSec + (30L * 86400L); // 30 days
        long ubSec1d = lbSec + (1L * 86400L);   // 1 day

        List<Integer> sensorIds = List.of(1, 2, 3);

        QueryRoutingDecision macroDecision = queryRouter.resolve(new TimeRange(lbSec, ubSec30d), sensorIds, 1920);
        assertThat(macroDecision.targetTable()).isEqualTo("iot.sensor_rollup_1h");
        assertThat(macroDecision.isMacro30Day()).isTrue();
        assertThat(macroDecision.sqlQuery()).contains("iot.sensor_rollup_1h");
        assertThat(macroDecision.sqlQuery()).contains("sensor_id IN (1, 2, 3)");

        QueryRoutingDecision midDecision = queryRouter.resolve(new TimeRange(lbSec, ubSec1d), sensorIds, 1920);
        assertThat(midDecision.targetTable()).isEqualTo("iot.sensor_rollup_1m");
        assertThat(midDecision.isMacro30Day()).isFalse();
        assertThat(midDecision.sqlQuery()).contains("iot.sensor_rollup_1m");
    }

    @Test
    @DisplayName("Streams multi-sensor Arrow IPC data sorted by sensor_id and timestamp for 30-day macro view")
    void testMultiSensor30DayArrowStream() throws Exception {
        long lbSec = 1729728000L;
        long ubSec30d = lbSec + (30L * 86400L);

        MvcResult asyncResult = mockMvc.perform(get("/api/v1/telemetry/am4-arrow")
                        .param("sensor_ids", "1,2,3")
                        .param("lb_sec", String.valueOf(lbSec))
                        .param("ub_sec", String.valueOf(ubSec30d))
                        .param("width", "1920"))
                .andExpect(request().asyncStarted())
                .andReturn();

        MvcResult result = mockMvc.perform(asyncDispatch(asyncResult))
                .andExpect(status().isOk())
                .andExpect(header().string("Content-Type", "application/vnd.apache.arrow.stream"))
                .andExpect(header().doesNotExist("Content-Encoding"))
                .andReturn();

        byte[] body = result.getResponse().getContentAsByteArray();
        assertThat(body).isNotEmpty();

        try (RootAllocator allocator = new RootAllocator();
             ByteArrayInputStream in = new ByteArrayInputStream(body);
             ArrowStreamReader reader = new ArrowStreamReader(in, allocator)) {

            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            boolean hasBatch = reader.loadNextBatch();
            assertThat(hasBatch).isTrue();

            UInt4Vector sensorIdVec = (UInt4Vector) root.getVector("sensor_id");
            Float8Vector tsVec = (Float8Vector) root.getVector("ts_ms");

            int rowCount = root.getRowCount();
            assertThat(rowCount).isGreaterThan(10);

            Set<Integer> uniqueSensors = new HashSet<>();
            int lastSensorId = -1;
            double lastTs = -1.0;

            for (int i = 0; i < rowCount; i++) {
                int sId = sensorIdVec.getObject(i);
                double ts = tsVec.get(i);
                uniqueSensors.add(sId);

                // Check sorting: sensor_id ASC, ts_ms ASC
                if (sId == lastSensorId) {
                    assertThat(ts).isGreaterThanOrEqualTo(lastTs);
                } else {
                    assertThat(sId).isGreaterThan(lastSensorId);
                }
                lastSensorId = sId;
                lastTs = ts;
            }

            assertThat(uniqueSensors).containsExactlyInAnyOrder(1, 2, 3);
        }
    }
}
