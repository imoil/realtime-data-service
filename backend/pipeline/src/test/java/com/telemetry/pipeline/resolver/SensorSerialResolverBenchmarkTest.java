package com.telemetry.pipeline.resolver;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;

import static org.assertj.core.api.Assertions.assertThat;

class SensorSerialResolverBenchmarkTest {

    private SensorSerialResolver resolver;
    private SensorSerialResolver.DeviceRoutingTable table;

    @BeforeEach
    void setUp() {
        resolver = new SensorSerialResolver();
        resolver.registerDeviceRange("DEVICE-001", 1000, 10001);
        table = resolver.getOrCreateDeviceTable("DEVICE-001");
    }

    @Test
    @DisplayName("SensorSerialResolver 정상 상태(steady-state) 조회 시 힙 메모리 할당이 정확히 0 바이트여야 한다")
    void testZeroHeapAllocationsDuringSteadyStateLookups() {
        com.sun.management.ThreadMXBean threadBean =
                (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();

        assertThat(threadBean.isThreadAllocatedMemorySupported()).isTrue();

        int blackhole = 0;
        for (int w = 0; w < 5; w++) {
            for (int i = 0; i < 500_000; i++) {
                blackhole += table.resolve(i % 1000);
            }
        }

        int iterations = 1_000_000;
        long minLevel2Allocated = Long.MAX_VALUE;
        for (int trial = 0; trial < 5; trial++) {
            long bytesBefore = threadBean.getCurrentThreadAllocatedBytes();
            for (int i = 0; i < iterations; i++) {
                blackhole += table.resolve(i % 1000);
            }
            long bytesAfter = threadBean.getCurrentThreadAllocatedBytes();
            long trialAllocated = bytesAfter - bytesBefore;
            if (trialAllocated < minLevel2Allocated) {
                minLevel2Allocated = trialAllocated;
            }
        }

        System.out.printf("Level 2 Array Lookup: %d iterations, min steady-state allocated bytes: %d, blackhole: %d%n",
                iterations, minLevel2Allocated, blackhole);

        assertThat(minLevel2Allocated)
                .as("Steady-state resolution in Level 2 routing table must cause 0 heap allocations")
                .isEqualTo(0L);

        String cachedDeviceId = "DEVICE-001";
        for (int w = 0; w < 5; w++) {
            for (int i = 0; i < 500_000; i++) {
                blackhole += resolver.resolve(cachedDeviceId, i % 1000);
            }
        }

        long minL1Allocated = Long.MAX_VALUE;
        for (int trial = 0; trial < 5; trial++) {
            long l1BytesBefore = threadBean.getCurrentThreadAllocatedBytes();
            for (int i = 0; i < iterations; i++) {
                blackhole += resolver.resolve(cachedDeviceId, i % 1000);
            }
            long l1BytesAfter = threadBean.getCurrentThreadAllocatedBytes();
            long trialAllocated = l1BytesAfter - l1BytesBefore;
            if (trialAllocated < minL1Allocated) {
                minL1Allocated = trialAllocated;
            }
        }

        System.out.printf("Level 1 + Level 2 Lookup: %d iterations, min steady-state allocated bytes: %d, blackhole: %d%n",
                iterations, minL1Allocated, blackhole);

        assertThat(minL1Allocated)
                .as("Steady-state resolution via resolver.resolve must cause 0 heap allocations")
                .isEqualTo(0L);
    }

    @Test
    @DisplayName("SensorSerialResolver L1 캐시 직접 배열 조회 지연 시간이 2ns 미만이어야 한다")
    void testSub2NanosecondResolutionLatency() {
        int blackhole = 0;
        for (int i = 0; i < 2_000_000; i++) {
            blackhole += table.resolve(i % 1000);
        }

        int iterations = 10_000_000;
        long startNano = System.nanoTime();
        for (int i = 0; i < iterations; i++) {
            blackhole += table.resolve(i % 1000);
        }
        long endNano = System.nanoTime();

        long totalDurationNano = endNano - startNano;
        double latencyPerLookupNs = (double) totalDurationNano / iterations;

        System.out.printf("SensorSerialResolver Benchmark: %d lookups in %d ms (%.3f ns/op), blackhole: %d%n",
                iterations, totalDurationNano / 1_000_000, latencyPerLookupNs, blackhole);

        assertThat(latencyPerLookupNs)
                .as("Steady-state lookup latency in Level 2 table must be < 2.0 ns")
                .isLessThan(2.0);
    }
}
