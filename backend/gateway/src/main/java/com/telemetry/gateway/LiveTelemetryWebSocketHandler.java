package com.telemetry.gateway;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.util.ArrayList;
import java.util.List;

/**
 * WebSocket handler managing client subscriptions for live telemetry.
 */
@Component
public class LiveTelemetryWebSocketHandler extends TextWebSocketHandler {

    private static final Logger log = LoggerFactory.getLogger(LiveTelemetryWebSocketHandler.class);

    private final LiveTelemetrySubscriptionRegistry registry;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public LiveTelemetryWebSocketHandler(LiveTelemetrySubscriptionRegistry registry) {
        this.registry = registry;
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession session) {
        log.info("Live telemetry WebSocket connection established: {}", session.getId());
    }

    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) throws Exception {
        String payload = message.getPayload();
        try {
            JsonNode root = objectMapper.readTree(payload);
            String action = root.path("action").asText("subscribe");
            List<Integer> sensorIds = new ArrayList<>();

            JsonNode idsNode = root.path("sensorIds");
            if (idsNode.isArray()) {
                for (JsonNode id : idsNode) {
                    sensorIds.add(id.asInt());
                }
            } else if (root.has("sensorId")) {
                sensorIds.add(root.path("sensorId").asInt());
            }

            if (!sensorIds.isEmpty()) {
                if ("unsubscribe".equalsIgnoreCase(action)) {
                    registry.unsubscribe(session, sensorIds);
                    log.info("Session {} unsubscribed from sensors {}", session.getId(), sensorIds);
                } else {
                    registry.subscribe(session, sensorIds);
                    log.info("Session {} subscribed to sensors {}", session.getId(), sensorIds);
                }
            }
        } catch (Exception e) {
            log.warn("Failed to parse WebSocket incoming message: {}", payload, e);
        }
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        log.info("Live telemetry WebSocket closed: {} with status {}", session.getId(), status);
        registry.removeSession(session.getId());
    }

    @Override
    public void handleTransportError(WebSocketSession session, Throwable exception) {
        log.warn("Transport error on WebSocket session {}: {}", session.getId(), exception.getMessage());
        registry.removeSession(session.getId());
    }
}
