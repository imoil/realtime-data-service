package com.telemetry.gateway;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Simulates incoming 1-second Kafka sensor stream for actively subscribed sensors.
 */
@Service
@EnableScheduling
public class LiveTelemetrySimulator {

    private static final Logger log = LoggerFactory.getLogger(LiveTelemetrySimulator.class);

    private final LiveTelemetrySubscriptionRegistry registry;
    private final LiveTelemetryBroadcastService broadcastService;

    public LiveTelemetrySimulator(
            LiveTelemetrySubscriptionRegistry registry,
            LiveTelemetryBroadcastService broadcastService
    ) {
        this.registry = registry;
        this.broadcastService = broadcastService;
    }

    @Scheduled(fixedRate = 1000)
    public void generateLiveTicks() {
        long nowMs = Instant.now().toEpochMilli();

        // Broadcast a live 1-second tick to every sensor with active subscribers
        for (int sensorId = 1; sensorId <= 50; sensorId++) {
            if (!registry.getSessionsForSensor(sensorId).isEmpty()) {
                double val = 20.0 + (sensorId % 10) * 3.5 + (ThreadLocalRandom.current().nextDouble() - 0.5) * 4.0;
                broadcastService.broadcastTick(new TelemetryTick(sensorId, nowMs, val));
            }
        }
    }
}
