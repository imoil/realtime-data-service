package com.telemetry.gateway;

import com.google.protobuf.CodedInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * High-throughput Kafka-to-WebSocket live tail consumer bridge.
 *
 * Listens to incoming high-frequency telemetry records from Kafka,
 * filters against the active in-memory subscription registry (O(1)),
 * and fans out matching live ticks to client WebSockets with <100ms latency
 * via Java 21 Virtual Threads.
 */
@Service
public class KafkaLiveTelemetryBridgeService {

    private static final Logger log = LoggerFactory.getLogger(KafkaLiveTelemetryBridgeService.class);

    private static final int FIELD_SENSOR_ID = 1;
    private static final int FIELD_TIMESTAMP_MS = 2;
    private static final int FIELD_VALUE = 3;

    private final LiveTelemetrySubscriptionRegistry registry;
    private final LiveTelemetryBroadcastService broadcastService;
    private final boolean kafkaEnabled;
    private final ExecutorService virtualThreadExecutor;

    private final AtomicLong processedCount = new AtomicLong(0);
    private final AtomicLong dispatchedCount = new AtomicLong(0);
    private final AtomicLong droppedCount = new AtomicLong(0);

    public KafkaLiveTelemetryBridgeService(
            LiveTelemetrySubscriptionRegistry registry,
            LiveTelemetryBroadcastService broadcastService,
            @Value("${telemetry.kafka.enabled:false}") boolean kafkaEnabled
    ) {
        this.registry = registry;
        this.broadcastService = broadcastService;
        this.kafkaEnabled = kafkaEnabled;
        this.virtualThreadExecutor = Executors.newVirtualThreadPerTaskExecutor();
    }

    public boolean isKafkaEnabled() {
        return kafkaEnabled;
    }

    /**
     * Spring Kafka listener method. Auto-starts only when telemetry.kafka.enabled=true.
     */
    @KafkaListener(
            topics = "${telemetry.kafka.topic:iot.telemetry.raw}",
            groupId = "${telemetry.kafka.group-id:telemetry-gateway-live-tail}",
            autoStartup = "${telemetry.kafka.enabled:false}"
    )
    public void onKafkaRecord(byte[] payload) {
        processRawMessage(payload);
    }

    /**
     * Ingests and processes a raw Protobuf byte payload.
     * Decodes sensor ID first to perform early O(1) filter drop before full parsing.
     */
    public boolean processRawMessage(byte[] payload) {
        processedCount.incrementAndGet();
        if (payload == null || payload.length == 0) {
            droppedCount.incrementAndGet();
            return false;
        }

        try {
            CodedInputStream input = CodedInputStream.newInstance(payload);
            int sensorId = 0;
            long timestampMs = 0L;
            double value = 0.0;

            while (!input.isAtEnd()) {
                int tag = input.readTag();
                if (tag == 0) break;
                int fieldNumber = tag >>> 3;
                switch (fieldNumber) {
                    case FIELD_SENSOR_ID -> {
                        sensorId = input.readUInt32();
                        // Early filter: if no clients are subscribed to this sensor, drop immediately
                        if (!registry.hasActiveSubscribers(sensorId)) {
                            droppedCount.incrementAndGet();
                            return false;
                        }
                    }
                    case FIELD_TIMESTAMP_MS -> timestampMs = input.readInt64();
                    case FIELD_VALUE -> value = input.readDouble();
                    default -> input.skipField(tag);
                }
            }

            TelemetryTick tick = new TelemetryTick(sensorId, timestampMs, value);
            return dispatchTick(tick);
        } catch (IOException e) {
            log.warn("Failed to decode Protobuf telemetry tick from Kafka: {}", e.getMessage());
            droppedCount.incrementAndGet();
            return false;
        }
    }

    /**
     * Directly processes a parsed tick.
     */
    public boolean processTick(TelemetryTick tick) {
        processedCount.incrementAndGet();
        if (!registry.hasActiveSubscribers(tick.sensorId())) {
            droppedCount.incrementAndGet();
            return false;
        }
        return dispatchTick(tick);
    }

    private boolean dispatchTick(TelemetryTick tick) {
        dispatchedCount.incrementAndGet();
        virtualThreadExecutor.execute(() -> {
            try {
                broadcastService.broadcastTick(tick);
            } catch (Exception e) {
                log.warn("Error broadcasting live tick for sensor {}: {}", tick.sensorId(), e.getMessage());
            }
        });
        return true;
    }

    public long getProcessedCount() {
        return processedCount.get();
    }

    public long getDispatchedCount() {
        return dispatchedCount.get();
    }

    public long getDroppedCount() {
        return droppedCount.get();
    }
}
