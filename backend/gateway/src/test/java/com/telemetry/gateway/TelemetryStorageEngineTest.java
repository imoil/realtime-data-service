package com.telemetry.gateway;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
class TelemetryStorageEngineTest {

    @Autowired
    private TelemetryStorageEngine storageEngine;

    @Test
    @DisplayName("Streams micro-zoom raw measurements (<= 2Ws) from iot.sensor_iceberg_raw")
    void testStreamTelemetryMicroZoomRaw() throws Exception {
        long lbSec = 1729728000L;
        long ubSec = lbSec + 120L; // 2 minutes <= 2 * 1920 = 3840s -> raw mode

        TelemetryQueryRequest request = new TelemetryQueryRequest(
                List.of(1, 2),
                new TimeRange(lbSec, ubSec),
                1920
        );

        TelemetryStreamResult result = storageEngine.streamTelemetry(request);

        assertThat(result.targetTable()).isEqualTo("iot.sensor_iceberg_raw");
        assertThat(result.queryMode()).isEqualTo("raw");
        assertThat(result.pointCount()).isGreaterThan(0);

        // Verify that StreamingResponseBody outputs valid Arrow IPC stream
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        result.responseBody().writeTo(out);

        byte[] arrowBytes = out.toByteArray();
        assertThat(arrowBytes).isNotEmpty();

        try (RootAllocator allocator = new RootAllocator();
             ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(arrowBytes), allocator)) {
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            List<String> fieldNames = root.getSchema().getFields().stream().map(Field::getName).toList();
            assertThat(fieldNames).containsExactly("sensor_id", "ts_ms", "val");
            assertThat(reader.loadNextBatch()).isTrue();
            assertThat(root.getRowCount()).isEqualTo(result.pointCount());
        }
    }

    @Test
    @DisplayName("Streams micro-zoom AM4 downsampled data (2Ws < ΔT <= 6h) from iot.sensor_iceberg_raw")
    void testStreamTelemetryMicroZoomAm4() {
        long lbSec = 1729728000L;
        long ubSec = lbSec + (5L * 3600L); // 5 hours -> AM4 mode on Iceberg

        TelemetryQueryRequest request = new TelemetryQueryRequest(
                List.of(1),
                new TimeRange(lbSec, ubSec),
                1920
        );

        TelemetryStreamResult result = storageEngine.streamTelemetry(request);

        assertThat(result.targetTable()).isEqualTo("iot.sensor_iceberg_raw");
        assertThat(result.queryMode()).isEqualTo("am4");
        assertThat(result.pointCount()).isEqualTo(1920 * 4);
    }

    @Test
    @DisplayName("Streams intermediate data (6h < ΔT <= 7d) from iot.sensor_rollup_1m")
    void testStreamTelemetryIntermediate() {
        long lbSec = 1729728000L;
        long ubSec = lbSec + (2L * 86400L); // 2 days

        TelemetryQueryRequest request = new TelemetryQueryRequest(
                List.of(1, 2, 3),
                new TimeRange(lbSec, ubSec),
                1920
        );

        TelemetryStreamResult result = storageEngine.streamTelemetry(request);

        assertThat(result.targetTable()).isEqualTo("iot.sensor_rollup_1m");
        assertThat(result.queryMode()).isEqualTo("am4");
        assertThat(result.pointCount()).isGreaterThan(0);
    }

    @Test
    @DisplayName("Streams macro data (ΔT > 7d) from iot.sensor_rollup_1h")
    void testStreamTelemetryMacro() {
        long lbSec = 1729728000L;
        long ubSec = lbSec + (30L * 86400L); // 30 days

        TelemetryQueryRequest request = new TelemetryQueryRequest(
                List.of(1, 2, 3),
                new TimeRange(lbSec, ubSec),
                1920
        );

        TelemetryStreamResult result = storageEngine.streamTelemetry(request);

        assertThat(result.targetTable()).isEqualTo("iot.sensor_rollup_1h");
        assertThat(result.queryMode()).isEqualTo("am4");
        assertThat(result.pointCount()).isGreaterThan(0);
    }
}
