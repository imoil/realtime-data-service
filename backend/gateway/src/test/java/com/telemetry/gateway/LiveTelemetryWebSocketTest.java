package com.telemetry.gateway;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.web.socket.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.security.Principal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class LiveTelemetryWebSocketTest {

    private LiveTelemetrySubscriptionRegistry registry;
    private LiveTelemetryBroadcastService broadcastService;

    @BeforeEach
    void setUp() {
        registry = new LiveTelemetrySubscriptionRegistry();
        broadcastService = new LiveTelemetryBroadcastService(registry);
    }

    @Test
    @DisplayName("Broadcasts live tick only to subscribed WebSocket sessions")
    void testFilteredBroadcast() {
        FakeWebSocketSession sessionA = new FakeWebSocketSession("session-A");
        FakeWebSocketSession sessionB = new FakeWebSocketSession("session-B");

        // Session A subscribes to sensors 1 and 2
        registry.subscribe(sessionA, List.of(1, 2));

        // Session B subscribes to sensor 3
        registry.subscribe(sessionB, List.of(3));

        assertThat(registry.getSubscribedSensors("session-A")).containsExactlyInAnyOrder(1, 2);
        assertThat(registry.getSubscribedSensors("session-B")).containsExactlyInAnyOrder(3);

        // Broadcast tick for sensor 1
        TelemetryTick tick1 = new TelemetryTick(1, 1729728001000L, 25.4);
        broadcastService.broadcastTick(tick1);

        // Session A should have received the message, Session B should NOT
        assertThat(sessionA.receivedMessages).hasSize(1);
        assertThat(sessionA.receivedMessages.get(0).getPayload()).contains("\"sensorId\":1");
        assertThat(sessionB.receivedMessages).isEmpty();

        // Broadcast tick for sensor 3
        TelemetryTick tick3 = new TelemetryTick(3, 1729728001000L, 88.1);
        broadcastService.broadcastTick(tick3);

        // Session B should now receive, Session A still only received 1 message
        assertThat(sessionB.receivedMessages).hasSize(1);
        assertThat(sessionB.receivedMessages.get(0).getPayload()).contains("\"sensorId\":3");
        assertThat(sessionA.receivedMessages).hasSize(1);
    }

    @Test
    @DisplayName("Cleans up subscriptions when WebSocket session disconnects")
    void testSessionDisconnectCleanup() {
        FakeWebSocketSession session = new FakeWebSocketSession("session-X");

        registry.subscribe(session, List.of(10, 20));
        assertThat(registry.getSubscribedSensors("session-X")).isNotEmpty();
        assertThat(registry.getSessionsForSensor(10)).contains(session);

        // Disconnect
        registry.removeSession("session-X");

        assertThat(registry.getSubscribedSensors("session-X")).isEmpty();
        assertThat(registry.getSessionsForSensor(10)).doesNotContain(session);
    }

    private static class FakeWebSocketSession implements WebSocketSession {
        private final String id;
        public final List<TextMessage> receivedMessages = new ArrayList<>();
        private boolean open = true;

        public FakeWebSocketSession(String id) {
            this.id = id;
        }

        @Override public String getId() { return id; }
        @Override public URI getUri() { return URI.create("/ws/v1/telemetry/live"); }
        @Override public HttpHeaders getHandshakeHeaders() { return new HttpHeaders(); }
        @Override public Map<String, Object> getAttributes() { return Map.of(); }
        @Override public Principal getPrincipal() { return null; }
        @Override public InetSocketAddress getLocalAddress() { return null; }
        @Override public InetSocketAddress getRemoteAddress() { return null; }
        @Override public String getAcceptedProtocol() { return null; }
        @Override public void setTextMessageSizeLimit(int messageSizeLimit) {}
        @Override public int getTextMessageSizeLimit() { return 65536; }
        @Override public void setBinaryMessageSizeLimit(int messageSizeLimit) {}
        @Override public int getBinaryMessageSizeLimit() { return 65536; }
        @Override public List<WebSocketExtension> getExtensions() { return List.of(); }

        @Override
        public void sendMessage(WebSocketMessage<?> message) throws IOException {
            if (message instanceof TextMessage tm) {
                receivedMessages.add(tm);
            }
        }

        @Override public boolean isOpen() { return open; }
        @Override public void close() { this.open = false; }
        @Override public void close(CloseStatus status) { this.open = false; }
    }
}
