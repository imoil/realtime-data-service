package com.telemetry.pipeline;

import java.io.Serializable;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;

/**
 * Encapsulates the Apache Iceberg Table specification and partition pruning logic
 * for the raw telemetry lakehouse storage layer (iot.sensor_iceberg_raw).
 *
 * Partition Spec:
 *   - bucket(32, sensor_id)
 *   - hours(ts)
 *
 * Storage & Distribution:
 *   - write.distribution-mode: hash
 *   - write.format.default: parquet
 *   - write.parquet.compression-codec: zstd
 *   - write.target-file-size-bytes: 268435456 (256MB)
 *   - sort order: (sensor_id ASC, ts ASC)
 */
public class IcebergRawTableConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    public static final String NAMESPACE = "iot";
    public static final String TABLE_NAME = "sensor_iceberg_raw";
    public static final String FULL_TABLE_NAME = NAMESPACE + "." + TABLE_NAME;

    public static final int NUM_BUCKETS = 32;
    public static final long TARGET_FILE_SIZE_BYTES = 256L * 1024L * 1024L; // 256MB
    public static final String COMPRESSION_CODEC = "zstd";
    public static final String DISTRIBUTION_MODE = "hash";
    public static final String CATALOG_TYPE = "rest";
    public static final String DEFAULT_CATALOG_URI = "http://polaris:8181/api/catalog";
    public static final String DEFAULT_WAREHOUSE = "s3://telemetry-lakehouse/iceberg";

    /**
     * Computes the deterministic hash bucket for a given sensor_id using 32 buckets.
     */
    public static int computeBucket(int sensorId) {
        return Math.floorMod(Integer.hashCode(sensorId), NUM_BUCKETS);
    }

    /**
     * Computes the epoch hour partition key for a timestamp in milliseconds.
     */
    public static long computeHourPartition(long timestampMs) {
        return timestampMs / (3600L * 1000L);
    }

    /**
     * Generates Iceberg Table Properties map according to ADR-001.
     */
    public static Map<String, String> getTableProperties() {
        Map<String, String> props = new LinkedHashMap<>();
        props.put("write.format.default", "parquet");
        props.put("write.parquet.compression-codec", COMPRESSION_CODEC);
        props.put("write.target-file-size-bytes", String.valueOf(TARGET_FILE_SIZE_BYTES));
        props.put("write.distribution-mode", DISTRIBUTION_MODE);
        props.put("catalog-type", CATALOG_TYPE);
        props.put("catalog.uri", DEFAULT_CATALOG_URI);
        props.put("catalog.warehouse", DEFAULT_WAREHOUSE);
        return Collections.unmodifiableMap(props);
    }

    /**
     * Partition definition for partition pruning.
     */
    public record PartitionKey(int bucket, long hour) implements Comparable<PartitionKey> {
        @Override
        public int compareTo(PartitionKey o) {
            int c = Integer.compare(this.bucket, o.bucket);
            return (c != 0) ? c : Long.compare(this.hour, o.hour);
        }
    }

    /**
     * Calculates the pruned partition set required to answer a query for the given
     * sensor IDs and time window [lbSec, ubSec].
     * Any partition outside this set is guaranteed not to contain matching data.
     */
    public static Set<PartitionKey> prunePartitions(Collection<Integer> sensorIds, long lbSec, long ubSec) {
        if (sensorIds == null || sensorIds.isEmpty()) {
            return Collections.emptySet();
        }

        // Calculate target buckets
        Set<Integer> targetBuckets = new HashSet<>();
        for (int sensorId : sensorIds) {
            targetBuckets.add(computeBucket(sensorId));
        }

        // Calculate target hour range
        long startHour = computeHourPartition(lbSec * 1000L);
        long endHour = computeHourPartition(ubSec * 1000L);

        Set<PartitionKey> requiredPartitions = new TreeSet<>();
        for (int bucket : targetBuckets) {
            for (long hour = startHour; hour <= endHour; hour++) {
                requiredPartitions.add(new PartitionKey(bucket, hour));
            }
        }

        return requiredPartitions;
    }

    /**
     * Returns true if partition pruning eliminated at least one bucket
     * (pruning ratio < 1.0) for the target sensor query.
     */
    public static double computeBucketPruningRatio(Collection<Integer> sensorIds) {
        if (sensorIds == null || sensorIds.isEmpty()) {
            return 0.0;
        }
        Set<Integer> targetBuckets = new HashSet<>();
        for (int sensorId : sensorIds) {
            targetBuckets.add(computeBucket(sensorId));
        }
        return (double) targetBuckets.size() / NUM_BUCKETS;
    }
}
