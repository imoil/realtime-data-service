package com.telemetry.gateway;

import org.springframework.stereotype.Component;
import org.springframework.web.socket.WebSocketSession;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Thread-safe registry maintaining active WebSocket sessions and their subscribed sensor IDs.
 */
@Component
public class LiveTelemetrySubscriptionRegistry {

    private final ConcurrentHashMap<String, Set<Integer>> sessionToSensors = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, Set<WebSocketSession>> sensorToSessions = new ConcurrentHashMap<>();

    public void subscribe(WebSocketSession session, List<Integer> sensorIds) {
        String sessionId = session.getId();
        sessionToSensors.computeIfAbsent(sessionId, k -> ConcurrentHashMap.newKeySet()).addAll(sensorIds);

        for (int sensorId : sensorIds) {
            sensorToSessions.computeIfAbsent(sensorId, k -> ConcurrentHashMap.newKeySet()).add(session);
        }
    }

    public void unsubscribe(WebSocketSession session, List<Integer> sensorIds) {
        String sessionId = session.getId();
        Set<Integer> currentSensors = sessionToSensors.get(sessionId);
        if (currentSensors != null) {
            currentSensors.removeAll(sensorIds);
        }

        for (int sensorId : sensorIds) {
            Set<WebSocketSession> sessions = sensorToSessions.get(sensorId);
            if (sessions != null) {
                sessions.remove(session);
            }
        }
    }

    public void removeSession(String sessionId) {
        Set<Integer> subscribed = sessionToSensors.remove(sessionId);
        if (subscribed != null) {
            for (int sensorId : subscribed) {
                Set<WebSocketSession> sessions = sensorToSessions.get(sensorId);
                if (sessions != null) {
                    sessions.removeIf(s -> s.getId().equals(sessionId));
                }
            }
        }
    }

    public Set<WebSocketSession> getSessionsForSensor(int sensorId) {
        return sensorToSessions.getOrDefault(sensorId, Collections.emptySet());
    }

    public Set<Integer> getSubscribedSensors(String sessionId) {
        return sessionToSensors.getOrDefault(sessionId, Collections.emptySet());
    }
}
