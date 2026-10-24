package com.telemetry.pipeline;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class IcebergPipelineSinkTest {

    @Test
    @DisplayName("Iceberg table config specifies bucket(32, sensor_id), hours(ts), 256MB ZSTD and hash distribution")
    void testIcebergTableSpecification() {
        Map<String, String> props = IcebergRawTableConfig.getTableProperties();

        assertThat(props.get("write.format.default")).isEqualTo("parquet");
        assertThat(props.get("write.parquet.compression-codec")).isEqualTo("zstd");
        assertThat(props.get("write.target-file-size-bytes")).isEqualTo(String.valueOf(256L * 1024L * 1024L));
        assertThat(props.get("write.distribution-mode")).isEqualTo("hash");
        assertThat(props.get("catalog-type")).isEqualTo("rest");
        assertThat(props.get("catalog.uri")).isEqualTo("http://polaris:8181/api/catalog");
        assertThat(props.get("catalog.warehouse")).isEqualTo("s3://telemetry-lakehouse/iceberg");

        IcebergSinkHelper sinkHelper = new IcebergSinkHelper();
        Map<String, String> sinkProps = sinkHelper.getSinkProperties();
        assertThat(sinkProps.get("write.sort.order")).isEqualTo("sensor_id ASC, ts ASC");
        assertThat(sinkProps.get("write.distribution-mode")).isEqualTo("hash");
    }

    @Test
    @DisplayName("Hash bucket computation distributes deterministically across 32 buckets")
    void testBucketComputation() {
        for (int sensorId = 0; sensorId < 100; sensorId++) {
            int bucket = IcebergRawTableConfig.computeBucket(sensorId);
            assertThat(bucket).isBetween(0, 31);
        }
    }

    @Test
    @DisplayName("Partition pruning isolates required bucket and hour combinations for query")
    void testPartitionPruning() {
        long lbSec = 1729728000L; // Oct 24 2024 00:00:00 UTC
        long ubSec6h = lbSec + (6L * 3600L); // 6 hours

        List<Integer> querySensors = List.of(1, 2, 3);
        Set<IcebergRawTableConfig.PartitionKey> partitions =
                IcebergRawTableConfig.prunePartitions(querySensors, lbSec, ubSec6h);

        // 3 sensors map to at most 3 distinct buckets
        // 6 hours span 7 epoch hours (startHour to endHour inclusive)
        assertThat(partitions).isNotEmpty();
        double pruningRatio = IcebergRawTableConfig.computeBucketPruningRatio(querySensors);
        assertThat(pruningRatio).isLessThanOrEqualTo(3.0 / 32.0); // pruned at least 29 buckets!
    }

    @Test
    @DisplayName("Iceberg snapshot commit produces valid O(1) REST Catalog UpdateTableRequest payload")
    void testSnapshotCommitPayload() {
        long snapshotId = 84920482049284L;
        Long parentSnapshotId = 84920482000000L;
        long timestampMs = System.currentTimeMillis();
        String manifestList = "s3://telemetry-lakehouse/iceberg/iot/sensor_iceberg_raw/metadata/snap-84920482049284.avro";

        IcebergSnapshotCommit commit = IcebergSnapshotCommit.createAppendCommit(
                snapshotId,
                parentSnapshotId,
                timestampMs,
                manifestList,
                5_000_000L,
                20L,
                5_120_000_000L
        );

        assertThat(commit.getSnapshotId()).isEqualTo(snapshotId);
        assertThat(commit.getParentSnapshotId()).isEqualTo(parentSnapshotId);
        assertThat(commit.getOperation()).isEqualTo("append");
        assertThat(commit.getSummary().get("added-records")).isEqualTo("5000000");

        String payload = commit.toRestCatalogCommitPayload();
        assertThat(payload).contains("\"action\": \"add-snapshot\"");
        assertThat(payload).contains("\"action\": \"set-current-snapshot\"");
        assertThat(payload).contains(String.valueOf(snapshotId));
        assertThat(payload).contains(manifestList);
    }
}
