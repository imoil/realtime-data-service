package com.telemetry.gateway;

import org.springframework.stereotype.Component;

/**
 * ClickHouse Iceberg REST Catalog integration and local NVMe filesystem_cache configuration.
 *
 * Exposes:
 * - Table engine definition for iot.sensor_iceberg_raw connected to OSS REST Catalog.
 * - NVMe filesystem_cache settings ensuring repeated byte-range requests execute with 0 remote I/O.
 */
@Component
public class ClickHouseIcebergConfig {

    public static final String CACHE_NAME = "nvme_cache";
    public static final String CACHE_PATH = "/var/lib/clickhouse/nvme_cache/";
    public static final long CACHE_MAX_SIZE_BYTES = 500L * 1024L * 1024L * 1024L; // 500 GiB
    public static final long MAX_FILE_SEGMENT_SIZE = 256L * 1024L * 1024L; // 256 MiB (matching Iceberg Parquet target)

    public static final String REST_CATALOG_URL = "http://polaris:8181/api/catalog/v1/namespaces/iot/tables/sensor_iceberg_raw";

    /**
     * Generates ClickHouse DDL for creating the Iceberg table engine mapped to the REST Catalog.
     */
    public String getCreateIcebergTableDDL() {
        return String.format("""
                CREATE TABLE IF NOT EXISTS iot.sensor_iceberg_raw
                (
                    sensor_id UInt32,
                    ts_ms     Int64,
                    val       Float64
                )
                ENGINE = Iceberg('%s')
                SETTINGS
                    filesystem_cache_name = '%s',
                    enable_filesystem_cache_on_write_operations = 0,
                    read_from_filesystem_cache_if_exists = 1;
                """, REST_CATALOG_URL, CACHE_NAME);
    }

    /**
     * Generates ClickHouse filesystem_cache XML configuration for local NVMe caching.
     */
    public String getFilesystemCacheConfigXml() {
        return String.format("""
                <clickhouse>
                    <filesystem_caches>
                        <%s>
                            <path>%s</path>
                            <max_size>%d</max_size>
                            <max_file_segment_size>%d</max_file_segment_size>
                            <cache_on_write_operations>0</cache_on_write_operations>
                        </%s>
                    </filesystem_caches>
                </clickhouse>
                """, CACHE_NAME, CACHE_PATH, CACHE_MAX_SIZE_BYTES, MAX_FILE_SEGMENT_SIZE, CACHE_NAME);
    }
}
