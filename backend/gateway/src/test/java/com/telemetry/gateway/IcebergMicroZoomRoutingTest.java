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
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.asyncDispatch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.request;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
class IcebergMicroZoomRoutingTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private TelemetryQueryRouter queryRouter;

    @Autowired
    private ClickHouseIcebergConfig icebergConfig;

    @Autowired
    private ClickHouseTelemetryService telemetryService;

    @Test
    @DisplayName("Routes queries with ΔT <= 6h to iot.sensor_iceberg_raw with AM4 or raw query")
    void testQueryRouterRoutesToIcebergForMicroZoom() {
        long lbSec = 1729728000L;
        long ubSec6h = lbSec + (6L * 3600L); // 6 hours (21600s)
        long ubSec1h = lbSec + 3600L;        // 1 hour (3600s <= 2 * 1920 = 3840s -> raw)
        long ubSec2d = lbSec + (2L * 86400L); // 2 days -> rollup 1m
        long ubSec30d = lbSec + (30L * 86400L); // 30 days -> rollup 1h

        List<Integer> sensorIds = List.of(1, 2, 3);
        int width = 1920;

        // 6 hours query: routes to iot.sensor_iceberg_raw in AM4 mode
        QueryRoutingDecision decision6h = queryRouter.resolve(new TimeRange(lbSec, ubSec6h), sensorIds, width);
        assertThat(decision6h.targetTable()).isEqualTo("iot.sensor_iceberg_raw");
        assertThat(decision6h.isRaw()).isFalse();
        assertThat(decision6h.sqlQuery()).contains("iot.sensor_iceberg_raw");
        assertThat(decision6h.sqlQuery()).contains("sensor_id IN (1, 2, 3)");
        assertThat(decision6h.sqlQuery()).contains("am4_points");

        // 1 hour query: routes to iot.sensor_iceberg_raw in raw mode (3600s <= 2 * 1920s)
        QueryRoutingDecision decision1h = queryRouter.resolve(new TimeRange(lbSec, ubSec1h), sensorIds, width);
        assertThat(decision1h.targetTable()).isEqualTo("iot.sensor_iceberg_raw");
        assertThat(decision1h.isRaw()).isTrue();
        assertThat(decision1h.sqlQuery()).contains("iot.sensor_iceberg_raw");
        assertThat(decision1h.sqlQuery()).doesNotContain("am4_points"); // raw scan without downsampling

        // 2 days query: routes to iot.sensor_rollup_1m
        QueryRoutingDecision decision2d = queryRouter.resolve(new TimeRange(lbSec, ubSec2d), sensorIds, width);
        assertThat(decision2d.targetTable()).isEqualTo("iot.sensor_rollup_1m");

        // 30 days query: routes to iot.sensor_rollup_1h
        QueryRoutingDecision decision30d = queryRouter.resolve(new TimeRange(lbSec, ubSec30d), sensorIds, width);
        assertThat(decision30d.targetTable()).isEqualTo("iot.sensor_rollup_1h");
    }

    @Test
    @DisplayName("ClickHouse Iceberg REST Catalog DDL and filesystem_cache configuration are properly formatted")
    void testClickHouseIcebergConfiguration() {
        String ddl = icebergConfig.getCreateIcebergTableDDL();
        assertThat(ddl).contains("CREATE TABLE IF NOT EXISTS iot.sensor_iceberg_raw");
        assertThat(ddl).contains("ENGINE = Iceberg('http://polaris:8181/api/catalog/v1/namespaces/iot/tables/sensor_iceberg_raw')");
        assertThat(ddl).contains("filesystem_cache_name = 'nvme_cache'");
        assertThat(ddl).contains("read_from_filesystem_cache_if_exists = 1");

        String cacheXml = icebergConfig.getFilesystemCacheConfigXml();
        assertThat(cacheXml).contains("<nvme_cache>");
        assertThat(cacheXml).contains("<path>/var/lib/clickhouse/nvme_cache/</path>");
        assertThat(cacheXml).contains("<max_file_segment_size>268435456</max_file_segment_size>"); // 256MB
    }

    @Test
    @DisplayName("10-sensor 6-hour query returns in under 160ms (SLA)")
    void test10Sensor6HourQueryPerformanceSLA() {
        List<Integer> tenSensors = new ArrayList<>();
        for (int i = 1; i <= 10; i++) {
            tenSensors.add(i);
        }

        long lbSec = 1729728000L;
        long ubSec6h = lbSec + (6L * 3600L);
        TimeRange range6h = new TimeRange(lbSec, ubSec6h);
        int width = 1920;

        // Warm up JVM JIT compiler
        for (int i = 0; i < 5; i++) {
            telemetryService.getAm4Telemetry(tenSensors, range6h, width);
        }

        // Measure execution time
        long startNano = System.nanoTime();
        List<Am4DataPoint> points = telemetryService.getAm4Telemetry(tenSensors, range6h, width);
        long elapsedMs = (System.nanoTime() - startNano) / 1_000_000L;

        assertThat(points).isNotEmpty();
        // 10 sensors * 1920 buckets * 4 points = 76,800 points
        assertThat(points.size()).isEqualTo(10 * 1920 * 4);
        assertThat(elapsedMs).as("Query execution time must be under 160ms SLA").isLessThan(160L);
    }

    @Test
    @DisplayName("GET /api/v1/telemetry/am4-arrow returns X-Target-Table=iot.sensor_iceberg_raw for <=6h requests")
    void testEndpointReturnsIcebergTargetHeader() throws Exception {
        long lbSec = 1729728000L;
        long ubSec6h = lbSec + (6L * 3600L);

        MvcResult asyncResult = mockMvc.perform(get("/api/v1/telemetry/am4-arrow")
                        .param("sensor_ids", "1,2,3")
                        .param("lb_sec", String.valueOf(lbSec))
                        .param("ub_sec", String.valueOf(ubSec6h))
                        .param("width", "1920"))
                .andExpect(request().asyncStarted())
                .andReturn();

        MvcResult result = mockMvc.perform(asyncDispatch(asyncResult))
                .andExpect(status().isOk())
                .andExpect(header().string("Content-Type", "application/vnd.apache.arrow.stream"))
                .andExpect(header().string("X-Target-Table", "iot.sensor_iceberg_raw"))
                .andExpect(header().string("X-Query-Mode", "am4"))
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
            Float8Vector valVec = (Float8Vector) root.getVector("val");

            assertThat(root.getRowCount()).isGreaterThan(0);
            assertThat(sensorIdVec.get(0)).isBetween(1, 3);
            assertThat(valVec.get(0)).isGreaterThan(0.0);
        }
    }
}
