package com.telemetry.pipeline;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Configuration parameters for the Flink TelemetryStreamJob.
 * Parses CLI arguments (--key=value or --key value) with production defaults.
 */
public class TelemetryJobParameters implements Serializable {
    private static final long serialVersionUID = 1L;

    public static final String DEFAULT_KAFKA_BOOTSTRAP = "localhost:9092";
    public static final String DEFAULT_KAFKA_TOPIC = "iot.telemetry.raw";
    public static final String DEFAULT_KAFKA_GROUP = "telemetry-flink-ingest";
    public static final String DEFAULT_ICEBERG_CATALOG = "http://localhost:8181";
    public static final String DEFAULT_ICEBERG_WAREHOUSE = "s3://telemetry-warehouse/";
    public static final String DEFAULT_CLICKHOUSE_URL = "http://localhost:8123";
    public static final String DEFAULT_CLICKHOUSE_DATABASE = "iot";
    public static final long DEFAULT_CHECKPOINT_INTERVAL_MS = 60_000L;

    private final String kafkaBootstrapServers;
    private final String kafkaTopic;
    private final String kafkaGroupId;
    private final String icebergCatalogUri;
    private final String icebergWarehouse;
    private final String clickhouseUrl;
    private final String clickhouseDatabase;
    private final long checkpointIntervalMs;

    public TelemetryJobParameters(
            String kafkaBootstrapServers,
            String kafkaTopic,
            String kafkaGroupId,
            String icebergCatalogUri,
            String icebergWarehouse,
            String clickhouseUrl,
            String clickhouseDatabase,
            long checkpointIntervalMs
    ) {
        this.kafkaBootstrapServers = kafkaBootstrapServers;
        this.kafkaTopic = kafkaTopic;
        this.kafkaGroupId = kafkaGroupId;
        this.icebergCatalogUri = icebergCatalogUri;
        this.icebergWarehouse = icebergWarehouse;
        this.clickhouseUrl = clickhouseUrl;
        this.clickhouseDatabase = clickhouseDatabase;
        this.checkpointIntervalMs = checkpointIntervalMs;
    }

    public static TelemetryJobParameters fromArgs(String[] args) {
        Map<String, String> map = new HashMap<>();
        if (args != null) {
            for (int i = 0; i < args.length; i++) {
                String arg = args[i];
                if (arg.startsWith("--")) {
                    String clean = arg.substring(2);
                    if (clean.contains("=")) {
                        String[] parts = clean.split("=", 2);
                        map.put(parts[0], parts[1]);
                    } else if (i + 1 < args.length && !args[i + 1].startsWith("--")) {
                        map.put(clean, args[++i]);
                    } else {
                        map.put(clean, "true");
                    }
                }
            }
        }

        return new TelemetryJobParameters(
                map.getOrDefault("kafka.bootstrap.servers", DEFAULT_KAFKA_BOOTSTRAP),
                map.getOrDefault("kafka.topic", DEFAULT_KAFKA_TOPIC),
                map.getOrDefault("kafka.group.id", DEFAULT_KAFKA_GROUP),
                map.getOrDefault("iceberg.catalog.uri", DEFAULT_ICEBERG_CATALOG),
                map.getOrDefault("iceberg.warehouse", DEFAULT_ICEBERG_WAREHOUSE),
                map.getOrDefault("clickhouse.url", DEFAULT_CLICKHOUSE_URL),
                map.getOrDefault("clickhouse.database", DEFAULT_CLICKHOUSE_DATABASE),
                Long.parseLong(map.getOrDefault("checkpoint.interval.ms", String.valueOf(DEFAULT_CHECKPOINT_INTERVAL_MS)))
        );
    }

    public String getKafkaBootstrapServers() {
        return kafkaBootstrapServers;
    }

    public String getKafkaTopic() {
        return kafkaTopic;
    }

    public String getKafkaGroupId() {
        return kafkaGroupId;
    }

    public String getIcebergCatalogUri() {
        return icebergCatalogUri;
    }

    public String getIcebergWarehouse() {
        return icebergWarehouse;
    }

    public String getClickhouseUrl() {
        return clickhouseUrl;
    }

    public String getClickhouseDatabase() {
        return clickhouseDatabase;
    }

    public long getCheckpointIntervalMs() {
        return checkpointIntervalMs;
    }
}
