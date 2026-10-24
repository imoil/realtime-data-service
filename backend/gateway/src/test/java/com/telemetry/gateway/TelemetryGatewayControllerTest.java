package com.telemetry.gateway;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import java.io.ByteArrayInputStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.asyncDispatch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.request;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
class TelemetryGatewayControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @Test
    @DisplayName("Streams uncompressed Apache Arrow IPC format with expected schema and extrema points")
    void testGetAm4ArrowStream() throws Exception {
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
                .andExpect(header().doesNotExist("Content-Encoding"))
                .andReturn();

        byte[] body = result.getResponse().getContentAsByteArray();
        assertThat(body).isNotEmpty();

        try (RootAllocator allocator = new RootAllocator();
             ByteArrayInputStream in = new ByteArrayInputStream(body);
             ArrowStreamReader reader = new ArrowStreamReader(in, allocator)) {

            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            assertThat(root.getSchema().getFields()).hasSize(3);

            assertThat(root.getSchema().getFields().get(0).getName()).isEqualTo("sensor_id");
            assertThat(root.getSchema().getFields().get(0).getType()).isInstanceOf(ArrowType.Int.class);

            assertThat(root.getSchema().getFields().get(1).getName()).isEqualTo("ts_ms");
            assertThat(root.getSchema().getFields().get(1).getType()).isInstanceOf(ArrowType.FloatingPoint.class);

            assertThat(root.getSchema().getFields().get(2).getName()).isEqualTo("val");
            assertThat(root.getSchema().getFields().get(2).getType()).isInstanceOf(ArrowType.FloatingPoint.class);

            boolean hasBatch = reader.loadNextBatch();
            assertThat(hasBatch).isTrue();
            assertThat(root.getRowCount()).isGreaterThan(0);

            UInt4Vector sensorIdVec = (UInt4Vector) root.getVector("sensor_id");
            Float8Vector tsVec = (Float8Vector) root.getVector("ts_ms");
            Float8Vector valVec = (Float8Vector) root.getVector("val");

            assertThat(sensorIdVec.getObject(0)).isEqualTo(1);
            assertThat(tsVec.get(0)).isGreaterThanOrEqualTo(1729728000000.0);
            assertThat(valVec.get(0)).isNotNull();
        }
    }
}
