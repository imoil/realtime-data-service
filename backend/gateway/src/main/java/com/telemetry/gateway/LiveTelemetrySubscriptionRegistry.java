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
            if (currentSensors.isEmpty()) {
                sessionToSensors.remove(sessionId);
            }
        }

        for (int sensorId : sensorIds) {
            Set<WebSocketSession> sessions = sensorToSessions.get(sensorId);
            if (sessions != null) {
                sessions.remove(session);
                if (sessions.isEmpty()) {
                    sensorToSessions.remove(sensorId);
                }
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
                    if (sessions.isEmpty()) {
                        sensorToSessions.remove(sensorId);
                    }
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

    /**
     * O(1) check if any active WebSocket client is currently subscribed to this sensor.
     * Crucial for filtering 5,000,000 raw sensors in the Kafka consumer thread with zero overhead.
     */
    public boolean hasActiveSubscribers(int sensorId) {
        Set<WebSocketSession> sessions = sensorToSessions.get(sensorId);
        return sessions != null && !sessions.isEmpty();
    }

    /**
     * Returns all sensor IDs that currently have at least one active subscriber.
     */
    public Set<Integer> getActiveSensorIds() {
        return Collections.unmodifiableSet(sensorToSessions.keySet());
    }

    public int getActiveSessionCount() {
        return sessionToSensors.size();
    }
}
