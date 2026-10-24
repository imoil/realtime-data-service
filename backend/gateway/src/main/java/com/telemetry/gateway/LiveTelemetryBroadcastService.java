package com.telemetry.gateway;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Service that filters real-time sensor ticks and broadcasts them to subscribed WebSocket sessions.
 */
@Service
public class LiveTelemetryBroadcastService {

    private static final Logger log = LoggerFactory.getLogger(LiveTelemetryBroadcastService.class);

    private final LiveTelemetrySubscriptionRegistry registry;

    public LiveTelemetryBroadcastService(LiveTelemetrySubscriptionRegistry registry) {
        this.registry = registry;
    }

    public void broadcastTick(TelemetryTick tick) {
        Set<WebSocketSession> sessions = registry.getSessionsForSensor(tick.sensorId());
        if (sessions == null || sessions.isEmpty()) {
            return;
        }

        TextMessage message = new TextMessage(tick.toJson());
        List<String> deadSessions = new ArrayList<>();

        for (WebSocketSession session : sessions) {
            if (session.isOpen()) {
                try {
                    synchronized (session) {
                        session.sendMessage(message);
                    }
                } catch (IOException e) {
                    log.warn("Failed to push live tick to session {}: {}", session.getId(), e.getMessage());
                    deadSessions.add(session.getId());
                }
            } else {
                deadSessions.add(session.getId());
            }
        }

        for (String deadSessionId : deadSessions) {
            registry.removeSession(deadSessionId);
        }
    }
}
