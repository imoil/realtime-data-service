package com.telemetry.gateway;

import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

/**
 * Result of a telemetry streaming operation containing the zero-copy response stream
 * and routing/optimization metadata for HTTP headers.
 */
public record TelemetryStreamResult(
        StreamingResponseBody responseBody,
        String targetTable,
        String queryMode, // "raw" or "am4"
        int pointCount
) {}
