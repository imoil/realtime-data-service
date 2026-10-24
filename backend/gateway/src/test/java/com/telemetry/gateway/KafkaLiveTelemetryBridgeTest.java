package com.telemetry.gateway;

import com.google.protobuf.CodedOutputStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpHeaders;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketExtension;
import org.springframework.web.socket.WebSocketMessage;
import org.springframework.web.socket.WebSocketSession;

import java.io.ByteArrayOutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
class KafkaLiveTelemetryBridgeTest {

    @Autowired
    private LiveTelemetrySubscriptionRegistry registry;

    @Autowired
    private LiveTelemetryBroadcastService broadcastService;

    @Autowired
    private KafkaLiveTelemetryBridgeService bridgeService;

    private DummyWebSocketSession session1;
    private DummyWebSocketSession session2;
    private final List<String> session1Messages = Collections.synchronizedList(new ArrayList<>());
    private final List<String> session2Messages = Collections.synchronizedList(new ArrayList<>());

    static class DummyWebSocketSession implements WebSocketSession {
        private final String id;
        private final List<String> received;
        private volatile boolean open = true;

        public DummyWebSocketSession(String id, List<String> received) {
            this.id = id;
            this.received = received;
        }

        @Override public String getId() { return id; }
        @Override public URI getUri() { return null; }
        @Override public HttpHeaders getHandshakeHeaders() { return new HttpHeaders(); }
        @Override public Map<String, Object> getAttributes() { return Collections.emptyMap(); }
        @Override public Principal getPrincipal() { return null; }
        @Override public InetSocketAddress getLocalAddress() { return null; }
        @Override public InetSocketAddress getRemoteAddress() { return null; }
        @Override public String getAcceptedProtocol() { return null; }
        @Override public void setTextMessageSizeLimit(int messageSizeLimit) {}
        @Override public int getTextMessageSizeLimit() { return 0; }
        @Override public void setBinaryMessageSizeLimit(int messageSizeLimit) {}
        @Override public int getBinaryMessageSizeLimit() { return 0; }
        @Override public List<WebSocketExtension> getExtensions() { return Collections.emptyList(); }
        @Override public boolean isOpen() { return open; }
        @Override public synchronized void sendMessage(WebSocketMessage<?> message) {
            if (message instanceof TextMessage textMsg) {
                received.add(textMsg.getPayload());
            }
        }
        @Override public void close() { open = false; }
        @Override public void close(CloseStatus status) { open = false; }
    }

    @BeforeEach
    void setUp() {
        session1Messages.clear();
        session2Messages.clear();

        session1 = new DummyWebSocketSession("ws-client-1", session1Messages);
        session2 = new DummyWebSocketSession("ws-client-2", session2Messages);

        // Clean registry
        registry.removeSession("ws-client-1");
        registry.removeSession("ws-client-2");
    }

    private byte[] encodeProtobuf(int sensorId, long tsMs, double val) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        CodedOutputStream out = CodedOutputStream.newInstance(baos);
        out.writeUInt32(1, sensorId);
        out.writeInt64(2, tsMs);
        out.writeDouble(3, val);
        out.flush();
        return baos.toByteArray();
    }

    @Test
    @DisplayName("Drops ticks for unsubscribed sensors with O(1) early filter")
    void testDropsUnsubscribedSensorTicks() {
        TelemetryTick tick = new TelemetryTick(999, 1729728000000L, 100.5);

        boolean dispatched = bridgeService.processTick(tick);

        assertThat(dispatched).isFalse();
        assertThat(session1Messages).isEmpty();
        assertThat(session2Messages).isEmpty();
    }

    @Test
    @DisplayName("Dispatches live ticks dynamically to subscribed clients without cross-talk")
    void testDynamicSubscriptionFilteringAndDispatch() throws Exception {
        // Client 1 subscribes to sensors [1, 2]
        registry.subscribe(session1, List.of(1, 2));

        // Client 2 subscribes to sensor [3]
        registry.subscribe(session2, List.of(3));

        // Ingest tick for sensor 1
        bridgeService.processTick(new TelemetryTick(1, 1729728001000L, 25.4));

        // Ingest tick for sensor 3
        bridgeService.processTick(new TelemetryTick(3, 1729728002000L, 88.9));

        // Ingest tick for sensor 99 (no subscribers)
        bridgeService.processTick(new TelemetryTick(99, 1729728003000L, 99.0));

        // Wait briefly for virtual thread execution
        Thread.sleep(80);

        // Client 1 should receive only sensor 1
        assertThat(session1Messages).hasSize(1);
        assertThat(session1Messages.get(0)).contains("\"sensorId\":1");

        // Client 2 should receive only sensor 3
        assertThat(session2Messages).hasSize(1);
        assertThat(session2Messages.get(0)).contains("\"sensorId\":3");
    }

    @Test
    @DisplayName("Decodes raw Kafka Protobuf bytes and forwards to subscribed sessions")
    void testProcessesRawProtobufKafkaPayload() throws Exception {
        registry.subscribe(session1, List.of(42));

        byte[] payload = encodeProtobuf(42, 1729728005000L, 42.42);

        boolean result = bridgeService.processRawMessage(payload);
        assertThat(result).isTrue();

        Thread.sleep(80);

        assertThat(session1Messages).hasSize(1);
        assertThat(session1Messages.get(0)).contains("\"sensorId\":42");
        assertThat(session1Messages.get(0)).contains("\"value\":42.4200");
    }

    @Test
    @DisplayName("Stops dispatching when client unsubscribes or disconnects")
    void testUnsubscribeAndDisconnectCleanup() throws Exception {
        registry.subscribe(session1, List.of(10));
        assertThat(registry.hasActiveSubscribers(10)).isTrue();

        // Tick while subscribed
        bridgeService.processTick(new TelemetryTick(10, 1729728000000L, 10.0));
        Thread.sleep(50);
        assertThat(session1Messages).hasSize(1);

        // Unsubscribe
        registry.unsubscribe(session1, List.of(10));
        assertThat(registry.hasActiveSubscribers(10)).isFalse();

        // Tick after unsubscribe
        boolean dispatched = bridgeService.processTick(new TelemetryTick(10, 1729728001000L, 10.1));
        assertThat(dispatched).isFalse();

        Thread.sleep(50);
        assertThat(session1Messages).hasSize(1); // Still 1, no new message
    }
}
