package com.telemetry.gateway;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

/**
 * High-performance ClickHouse HTTP client service for direct binary stream pass-through.
 *
 * Pipes FORMAT ArrowStream response bytes directly from ClickHouse HTTP socket
 * into the client's output stream without intermediate Java row object allocations,
 * maximizing throughput and eliminating GC pressure under heavy analytical load.
 */
@Service
public class ClickHouseClientService {

    private final boolean enabled;
    private final String url;
    private final String database;
    private final String user;
    private final String password;
    private final HttpClient httpClient;

    public ClickHouseClientService(
            @Value("${telemetry.clickhouse.enabled:false}") boolean enabled,
            @Value("${telemetry.clickhouse.url:http://localhost:8123}") String url,
            @Value("${telemetry.clickhouse.database:iot}") String database,
            @Value("${telemetry.clickhouse.user:default}") String user,
            @Value("${telemetry.clickhouse.password:}") String password
    ) {
        this.enabled = enabled;
        this.url = url;
        this.database = database;
        this.user = user;
        this.password = password;
        this.httpClient = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .connectTimeout(Duration.ofSeconds(3))
                .build();
    }

    public boolean isEnabled() {
        return enabled;
    }

    public String getUrl() {
        return url;
    }

    public String getDatabase() {
        return database;
    }

    /**
     * Executes the query against ClickHouse HTTP endpoint with FORMAT ArrowStream,
     * piping the raw response bytes directly to the output stream without Java allocations.
     *
     * @return true if stream was successfully executed and piped from ClickHouse, false otherwise.
     */
    public boolean streamDirectArrow(String sqlQuery, OutputStream outputStream) {
        if (!enabled) {
            return false;
        }

        try {
            String queryParam = URLEncoder.encode(sqlQuery, StandardCharsets.UTF_8);
            String fullUrl = String.format("%s/?database=%s&query=%s", url, database, queryParam);

            HttpRequest.Builder reqBuilder = HttpRequest.newBuilder()
                    .uri(URI.create(fullUrl))
                    .timeout(Duration.ofSeconds(10))
                    .GET();

            if (user != null && !user.isEmpty()) {
                reqBuilder.header("X-ClickHouse-User", user);
            }
            if (password != null && !password.isEmpty()) {
                reqBuilder.header("X-ClickHouse-Key", password);
            }

            HttpResponse<InputStream> response = httpClient.send(
                    reqBuilder.build(),
                    HttpResponse.BodyHandlers.ofInputStream()
            );

            if (response.statusCode() == 200) {
                try (InputStream in = response.body()) {
                    in.transferTo(outputStream);
                    outputStream.flush();
                    return true;
                }
            } else {
                System.err.println("ClickHouse query returned HTTP status: " + response.statusCode());
                return false;
            }
        } catch (Exception e) {
            System.err.println("ClickHouse direct streaming unavailable, falling back: " + e.getMessage());
            return false;
        }
    }
}
