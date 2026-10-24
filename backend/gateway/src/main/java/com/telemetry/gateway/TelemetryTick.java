package com.telemetry.gateway;

/**
 * 1-second live telemetry tick streamed directly over WebSocket.
 */
public record TelemetryTick(int sensorId, long timestampMs, double value) {

    public String toJson() {
        return String.format(
                "{\"sensorId\":%d,\"timestampMs\":%d,\"value\":%.4f}",
                sensorId, timestampMs, value
        );
    }
}
