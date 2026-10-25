package com.telemetry.pipeline.resolver;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class SensorSerialResolverTest {

    private SensorSerialResolver resolver;

    @BeforeEach
    void setUp() {
        resolver = new SensorSerialResolver();
    }

    @Test
    @DisplayName("사전 등록된 장치 센서 인덱스는 정확히 해당 일련번호로 매핑되어야 한다")
    void testRegisteredMapping() {
        resolver.registerMapping("DEVICE-001", 0, 10001);
        resolver.registerMapping("DEVICE-001", 1, 10002);
        resolver.registerMapping("DEVICE-002", 0, 10101);

        assertThat(resolver.resolve("DEVICE-001", 0)).isEqualTo(10001);
        assertThat(resolver.resolve("DEVICE-001", 1)).isEqualTo(10002);
        assertThat(resolver.resolve("DEVICE-002", 0)).isEqualTo(10101);

        assertThat(resolver.resolveSerialNumber("DEVICE-001", 0)).isEqualTo(10001L);
    }

    @Test
    @DisplayName("대소문자 및 공백이 포함된 장치 식별자도 동일한 키로 정규화되어 조회되어야 한다")
    void testCaseInsensitiveNormalization() {
        resolver.registerMapping("Device-001", 42, 10042);

        assertThat(resolver.resolve("device-001", 42)).isEqualTo(10042);
        assertThat(resolver.resolve("  DEVICE-001  ", 42)).isEqualTo(10042);
    }

    @Test
    @DisplayName("미등록 장치라도 결정론적 오프셋을 통해 서로 다른 장치 간 충돌이 발생하지 않아야 한다")
    void testDeterministicFallbackPreventsCollision() {
        int serialDev1 = resolver.resolve("DEVICE-001", 0);
        int serialDev2 = resolver.resolve("DEVICE-002", 0);
        int serialDev3 = resolver.resolve("DEVICE-003", 0);

        assertThat(serialDev1).isEqualTo(10001);
        assertThat(serialDev2).isEqualTo(11001);
        assertThat(serialDev3).isEqualTo(12001);

        assertThat(serialDev1).isNotEqualTo(serialDev2);
        assertThat(serialDev2).isNotEqualTo(serialDev3);
    }

    @Test
    @DisplayName("일괄 범위 등록이 정상 동작해야 한다")
    void testRegisterDeviceRange() {
        resolver.registerDeviceRange("TURBINE-01", 50, 20001);

        assertThat(resolver.resolve("TURBINE-01", 0)).isEqualTo(20001);
        assertThat(resolver.resolve("TURBINE-01", 49)).isEqualTo(20050);
        assertThat(resolver.getMappingsCount()).isEqualTo(50);
    }

    @Test
    @DisplayName("Copy-on-Write 동적 업데이트 중 다중 스레드에서 무중단 락프리 조회가 유지되어야 한다")
    void testConcurrentReadsDuringDynamicRegistration() throws InterruptedException {
        resolver.registerDeviceRange("DEVICE-001", 100, 10001);
        SensorSerialResolver.DeviceRoutingTable table = resolver.getOrCreateDeviceTable("DEVICE-001");

        int threadCount = 8;
        int iterations = 100_000;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);
        AtomicInteger errorCount = new AtomicInteger(0);

        for (int t = 0; t < threadCount; t++) {
            executor.submit(() -> {
                try {
                    for (int i = 0; i < iterations; i++) {
                        int idx = i % 100;
                        int resolved = table.resolve(idx);
                        if (resolved != (10001 + idx)) {
                            errorCount.incrementAndGet();
                        }
                    }
                } finally {
                    latch.countDown();
                }
            });
        }

        for (int i = 100; i < 200; i++) {
            table.register(i, 20000 + i);
        }

        boolean finished = latch.await(10, TimeUnit.SECONDS);
        executor.shutdown();

        assertThat(finished).isTrue();
        assertThat(errorCount.get()).isEqualTo(0);
        assertThat(table.resolve(150)).isEqualTo(20150);
    }
}
