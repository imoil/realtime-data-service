package com.telemetry.gateway;

import com.sun.net.httpserver.HttpServer;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.env.Environment;
import org.springframework.core.io.ClassPathResource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
class LiveClickHouseArrowStreamingTest {

    @Autowired
    private Environment environment;

    @Autowired
    private TelemetryStorageEngine storageEngine;

    @Autowired
    private ArrowStreamWriterService arrowWriterService;

    @Test
    @DisplayName("Spring Boot 3.4+ enables Java 21 Virtual Threads via spring.threads.virtual.enabled")
    void testVirtualThreadsConfigurationEnabled() {
        String virtualThreadsEnabled = environment.getProperty("spring.threads.virtual.enabled");
        assertThat(virtualThreadsEnabled).isEqualTo("true");
    }

    @Test
    @DisplayName("ClickHouse table migration DDL script exists and defines required 1m/1h and Iceberg tables")
    void testClickHouseMigrationDdlScriptValid() throws Exception {
        ClassPathResource resource = new ClassPathResource("db/migration/V1__init_telemetry_tables.sql");
        assertThat(resource.exists()).isTrue();

        try (InputStream in = resource.getInputStream()) {
            String ddlContent = new String(in.readAllBytes(), StandardCharsets.UTF_8);
            assertThat(ddlContent).contains("CREATE DATABASE IF NOT EXISTS iot;");
            assertThat(ddlContent).contains("CREATE TABLE IF NOT EXISTS iot.sensor_rollup_1m");
            assertThat(ddlContent).contains("CREATE TABLE IF NOT EXISTS iot.sensor_rollup_1h");
            assertThat(ddlContent).contains("CREATE TABLE IF NOT EXISTS iot.sensor_iceberg_raw");
            assertThat(ddlContent).contains("ENGINE = MergeTree()");
        }
    }

    @Test
    @DisplayName("ClickHouseClientService directly pipes live HTTP FORMAT ArrowStream bytes with zero-copy pass-through")
    void testClickHouseDirectArrowStreamingPassThrough() throws Exception {
        // Prepare sample Arrow IPC stream bytes
        List<Am4DataPoint> samplePoints = List.of(
                new Am4DataPoint(1, 1729728000000.0, 42.5),
                new Am4DataPoint(1, 1729728001000.0, 43.0)
        );
        ByteArrayOutputStream arrowSourceOut = new ByteArrayOutputStream();
        arrowWriterService.writeDataPoints(samplePoints, arrowSourceOut);
        byte[] expectedArrowBytes = arrowSourceOut.toByteArray();

        // Start lightweight mock ClickHouse HTTP server
        HttpServer mockServer = HttpServer.create(new InetSocketAddress(0), 0);
        int mockPort = mockServer.getAddress().getPort();

        mockServer.createContext("/", exchange -> {
            exchange.getResponseHeaders().set("Content-Type", "application/vnd.apache.arrow.stream");
            exchange.sendResponseHeaders(200, expectedArrowBytes.length);
            exchange.getResponseBody().write(expectedArrowBytes);
            exchange.getResponseBody().close();
        });
        mockServer.start();

        try {
            ClickHouseClientService liveClient = new ClickHouseClientService(
                    true, // enabled
                    "http://localhost:" + mockPort,
                    "iot",
                    "default",
                    ""
            );

            ByteArrayOutputStream destinationOut = new ByteArrayOutputStream();
            boolean success = liveClient.streamDirectArrow("SELECT * FROM iot.sensor_rollup_1m FORMAT ArrowStream", destinationOut);

            assertThat(success).isTrue();
            assertThat(destinationOut.toByteArray()).isEqualTo(expectedArrowBytes);

            // Verify piped bytes are valid Arrow IPC
            try (RootAllocator allocator = new RootAllocator();
                 ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(destinationOut.toByteArray()), allocator)) {
                VectorSchemaRoot root = reader.getVectorSchemaRoot();
                assertThat(reader.loadNextBatch()).isTrue();
                assertThat(root.getRowCount()).isEqualTo(2);
            }
        } finally {
            mockServer.stop(0);
        }
    }

    @Test
    @DisplayName("Falls back gracefully to in-memory generation when live ClickHouse is disabled or unreachable")
    void testGracefulFallbackWhenClickHouseDisabled() throws Exception {
        TelemetryQueryRequest request = new TelemetryQueryRequest(
                List.of(1),
                new TimeRange(1729728000L, 1729728000L + 60L),
                1920
        );

        TelemetryStreamResult result = storageEngine.streamTelemetry(request);
        assertThat(result).isNotNull();

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        result.responseBody().writeTo(out);

        byte[] bytes = out.toByteArray();
        assertThat(bytes).isNotEmpty();

        try (RootAllocator allocator = new RootAllocator();
             ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(bytes), allocator)) {
            assertThat(reader.loadNextBatch()).isTrue();
            assertThat(reader.getVectorSchemaRoot().getRowCount()).isGreaterThan(0);
        }
    }
}
