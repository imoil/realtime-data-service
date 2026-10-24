package com.telemetry.pipeline;

import org.apache.flink.streaming.api.datastream.DataStream;

import java.io.Serializable;
import java.util.Map;

/**
 * Helper for configuring the Apache Flink Iceberg Parquet Sink
 * for raw telemetry lakehouse storage.
 */
public class IcebergSinkHelper implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String catalogUri;
    private final String warehouse;
    private final String targetTable;
    private final int numBuckets;
    private final long targetFileSize;
    private final String compression;
    private final String distributionMode;

    public IcebergSinkHelper() {
        this(
                IcebergRawTableConfig.DEFAULT_CATALOG_URI,
                IcebergRawTableConfig.DEFAULT_WAREHOUSE,
                IcebergRawTableConfig.FULL_TABLE_NAME,
                IcebergRawTableConfig.NUM_BUCKETS,
                IcebergRawTableConfig.TARGET_FILE_SIZE_BYTES,
                IcebergRawTableConfig.COMPRESSION_CODEC,
                IcebergRawTableConfig.DISTRIBUTION_MODE
        );
    }

    public IcebergSinkHelper(
            String catalogUri,
            String warehouse,
            String targetTable,
            int numBuckets,
            long targetFileSize,
            String compression,
            String distributionMode
    ) {
        this.catalogUri = catalogUri;
        this.warehouse = warehouse;
        this.targetTable = targetTable;
        this.numBuckets = numBuckets;
        this.targetFileSize = targetFileSize;
        this.compression = compression;
        this.distributionMode = distributionMode;
    }

    public String getCatalogUri() {
        return catalogUri;
    }

    public String getWarehouse() {
        return warehouse;
    }

    public String getTargetTable() {
        return targetTable;
    }

    public int getNumBuckets() {
        return numBuckets;
    }

    public long getTargetFileSize() {
        return targetFileSize;
    }

    public String getCompression() {
        return compression;
    }

    public String getDistributionMode() {
        return distributionMode;
    }

    public Map<String, String> getSinkProperties() {
        return Map.of(
                "write.format.default", "parquet",
                "write.parquet.compression-codec", compression,
                "write.target-file-size-bytes", String.valueOf(targetFileSize),
                "write.distribution-mode", distributionMode,
                "write.sort.order", "sensor_id ASC, ts ASC"
        );
    }

    /**
     * Applies hash routing according to bucket(32, sensor_id)
     * to guarantee that each writer process handles a discrete partition bucket.
     */
    public DataStream<TelemetryRecord> routeByBucket(DataStream<TelemetryRecord> stream) {
        return stream.keyBy(record -> Math.floorMod(record.getSensorId(), numBuckets));
    }
}
