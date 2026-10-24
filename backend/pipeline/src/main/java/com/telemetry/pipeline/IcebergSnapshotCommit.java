package com.telemetry.pipeline;

import java.io.Serializable;
import java.util.*;

/**
 * Models atomic, O(1) snapshot commit operations registered with an OSS Iceberg REST Catalog
 * (such as Apache Polaris or Project Nessie).
 *
 * REST Catalog commits register new snapshots via an atomic metadata pointer swap,
 * avoiding directory tree crawls on S3 storage.
 */
public class IcebergSnapshotCommit implements Serializable {
    private static final long serialVersionUID = 1L;

    private final long snapshotId;
    private final Long parentSnapshotId;
    private final long timestampMillis;
    private final String manifestListLocation;
    private final String operation; // "append" or "overwrite"
    private final Map<String, String> summary;

    public IcebergSnapshotCommit(
            long snapshotId,
            Long parentSnapshotId,
            long timestampMillis,
            String manifestListLocation,
            String operation,
            Map<String, String> summary
    ) {
        this.snapshotId = snapshotId;
        this.parentSnapshotId = parentSnapshotId;
        this.timestampMillis = timestampMillis;
        this.manifestListLocation = Objects.requireNonNull(manifestListLocation, "manifestListLocation");
        this.operation = operation != null ? operation : "append";
        this.summary = summary != null ? Map.copyOf(summary) : Collections.emptyMap();
    }

    public static IcebergSnapshotCommit createAppendCommit(
            long snapshotId,
            Long parentSnapshotId,
            long timestampMillis,
            String manifestListLocation,
            long addedRecords,
            long addedFiles,
            long addedBytes
    ) {
        Map<String, String> sum = new LinkedHashMap<>();
        sum.put("operation", "append");
        sum.put("added-records", String.valueOf(addedRecords));
        sum.put("added-data-files", String.valueOf(addedFiles));
        sum.put("added-files-size", String.valueOf(addedBytes));
        sum.put("target-format", "parquet");
        sum.put("compression", IcebergRawTableConfig.COMPRESSION_CODEC);
        return new IcebergSnapshotCommit(snapshotId, parentSnapshotId, timestampMillis, manifestListLocation, "append", sum);
    }

    public long getSnapshotId() {
        return snapshotId;
    }

    public Long getParentSnapshotId() {
        return parentSnapshotId;
    }

    public long getTimestampMillis() {
        return timestampMillis;
    }

    public String getManifestListLocation() {
        return manifestListLocation;
    }

    public String getOperation() {
        return operation;
    }

    public Map<String, String> getSummary() {
        return summary;
    }

    /**
     * Builds the JSON payload conforming to the Iceberg REST Catalog specification
     * (UpdateTableRequest) for registering this snapshot in O(1) time.
     */
    public String toRestCatalogCommitPayload() {
        StringBuilder sb = new StringBuilder();
        sb.append("{\n");
        sb.append("  \"requirements\": [{\"type\": \"assert-table-uuid\"}],\n");
        sb.append("  \"updates\": [\n");
        sb.append("    {\n");
        sb.append("      \"action\": \"add-snapshot\",\n");
        sb.append("      \"snapshot\": {\n");
        sb.append("        \"snapshot-id\": ").append(snapshotId).append(",\n");
        if (parentSnapshotId != null) {
            sb.append("        \"parent-snapshot-id\": ").append(parentSnapshotId).append(",\n");
        }
        sb.append("        \"timestamp-ms\": ").append(timestampMillis).append(",\n");
        sb.append("        \"manifest-list\": \"").append(manifestListLocation).append("\",\n");
        sb.append("        \"summary\": {\n");
        sb.append("          \"operation\": \"").append(operation).append("\"");
        for (Map.Entry<String, String> entry : summary.entrySet()) {
            if (!"operation".equals(entry.getKey())) {
                sb.append(",\n          \"").append(entry.getKey()).append("\": \"").append(entry.getValue()).append("\"");
            }
        }
        sb.append("\n        }\n");
        sb.append("      }\n");
        sb.append("    },\n");
        sb.append("    {\n");
        sb.append("      \"action\": \"set-current-snapshot\",\n");
        sb.append("      \"snapshot-id\": ").append(snapshotId).append("\n");
        sb.append("    }\n");
        sb.append("  ]\n");
        sb.append("}");
        return sb.toString();
    }
}
