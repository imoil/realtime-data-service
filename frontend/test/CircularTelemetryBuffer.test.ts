import { describe, it, expect, beforeEach } from 'vitest';
import {
  CircularTelemetryBuffer,
  CircularTelemetryBufferPool
} from '../src/utils/CircularTelemetryBuffer';
import { useTelemetryDashboard } from '../src/composables/useTelemetryDashboard';

describe('CircularTelemetryBuffer', () => {
  it('initializes with fixed capacity and pre-allocated typed arrays', () => {
    const buffer = new CircularTelemetryBuffer(100);

    expect(buffer.capacity).toBe(100);
    expect(buffer.length).toBe(0);
    expect(buffer.getByteSize()).toBe(100 * 8 * 2); // 100 * 16 bytes = 1600 bytes
    expect(buffer.getOldest()).toBeNull();
    expect(buffer.getNewest()).toBeNull();
  });

  it('pushes samples and tracks ordered entries up to capacity', () => {
    const buffer = new CircularTelemetryBuffer(5);

    buffer.push(1000, 10.5);
    buffer.push(2000, 20.5);
    buffer.push(3000, 30.5);

    expect(buffer.length).toBe(3);
    expect(buffer.getOldest()).toEqual({ timestampMs: 1000, value: 10.5 });
    expect(buffer.getNewest()).toEqual({ timestampMs: 3000, value: 30.5 });
    expect(buffer.get(0)).toEqual({ timestampMs: 1000, value: 10.5 });
    expect(buffer.get(1)).toEqual({ timestampMs: 2000, value: 20.5 });
    expect(buffer.get(2)).toEqual({ timestampMs: 3000, value: 30.5 });
  });

  it('performs O(1) FIFO eviction on buffer overflow', () => {
    const buffer = new CircularTelemetryBuffer(3);

    buffer.push(1000, 10.0);
    buffer.push(2000, 20.0);
    buffer.push(3000, 30.0);
    expect(buffer.length).toBe(3);

    // Overflow by 1: 1000 should be evicted, [2000, 3000, 4000] remaining
    buffer.push(4000, 40.0);
    expect(buffer.length).toBe(3);
    expect(buffer.getOldest()).toEqual({ timestampMs: 2000, value: 20.0 });
    expect(buffer.getNewest()).toEqual({ timestampMs: 4000, value: 40.0 });
    expect(buffer.get(0)).toEqual({ timestampMs: 2000, value: 20.0 });
    expect(buffer.get(1)).toEqual({ timestampMs: 3000, value: 30.0 });
    expect(buffer.get(2)).toEqual({ timestampMs: 4000, value: 40.0 });

    // Overflow by 2: 2000 should be evicted, [3000, 4000, 5000] remaining
    buffer.push(5000, 50.0);
    expect(buffer.length).toBe(3);
    expect(buffer.getOldest()).toEqual({ timestampMs: 3000, value: 30.0 });
    expect(buffer.getNewest()).toEqual({ timestampMs: 5000, value: 50.0 });
  });

  it('exports contiguous ordered typed arrays across circular wrap-around', () => {
    const buffer = new CircularTelemetryBuffer(4);

    buffer.push(10, 1.0);
    buffer.push(20, 2.0);
    buffer.push(30, 3.0);
    buffer.push(40, 4.0);
    buffer.push(50, 5.0); // Evicts 10, wraps around
    buffer.push(60, 6.0); // Evicts 20, wraps around

    const contiguous = buffer.exportContiguous();
    expect(contiguous.length).toBe(4);
    expect(Array.from(contiguous.timestamps)).toEqual([30, 40, 50, 60]);
    expect(Array.from(contiguous.values)).toEqual([3.0, 4.0, 5.0, 6.0]);
  });

  it('resets without reallocation via clear()', () => {
    const buffer = new CircularTelemetryBuffer(10);
    buffer.push(100, 1.0);
    buffer.push(200, 2.0);

    expect(buffer.length).toBe(2);
    buffer.clear();

    expect(buffer.length).toBe(0);
    expect(buffer.getOldest()).toBeNull();
    expect(buffer.getNewest()).toBeNull();
  });
});

describe('CircularTelemetryBufferPool', () => {
  it('manages multi-sensor series within strict memory ceiling (<= 50MB for 50 sensors)', () => {
    const pool = new CircularTelemetryBufferPool({
      maxSensors: 50,
      capacityPerSensor: 10_000
    });

    for (let sensorId = 1; sensorId <= 50; sensorId++) {
      const buffer = pool.getOrCreateBuffer(sensorId);
      expect(buffer.capacity).toBe(10_000);
    }

    const totalBytes = pool.getTotalByteSize();
    const totalMb = totalBytes / (1024 * 1024);

    // 50 sensors * 10,000 points * 16 bytes = 8,000,000 bytes ≈ 7.63 MB
    expect(totalMb).toBeLessThanOrEqual(50.0);
    expect(totalBytes).toBe(50 * 10_000 * 16);
  });

  it('handles high-throughput continuous ingestion with zero memory growth', () => {
    const pool = new CircularTelemetryBufferPool({
      maxSensors: 5,
      capacityPerSensor: 1_000
    });

    const initialByteSize = pool.getTotalByteSize();

    // Ingest 100,000 samples continuously across 5 sensors (20x buffer overflow)
    for (let i = 0; i < 100_000; i++) {
      const sensorId = (i % 5) + 1;
      pool.push(sensorId, 1729728000000 + i, 20.0 + (i % 10));
    }

    // Memory footprint must remain strictly constant
    expect(pool.getTotalByteSize()).toBe(5 * 1_000 * 16);
    expect(pool.getTotalByteSize()).toBe(initialByteSize);

    // Each sensor buffer must be capped at 1,000 samples with correct newest values
    for (let sensorId = 1; sensorId <= 5; sensorId++) {
      const buf = pool.getOrCreateBuffer(sensorId);
      expect(buf.length).toBe(1_000);
      expect(buf.getNewest()?.timestampMs).toBeGreaterThan(1729728000000 + 99_000);
    }
  });
});

describe('useTelemetryDashboard Circular Buffer Integration', () => {
  beforeEach(() => {
    const mockWs = {
      readyState: 1,
      send: () => {},
      close: () => {},
      addEventListener: () => {},
      removeEventListener: () => {}
    };
    // @ts-expect-error mock WebSocket in test
    globalThis.WebSocket = () => mockWs;
  });

  it('ingests live ticks into fixed-budget circular ring buffer pool', () => {
    const dashboard = useTelemetryDashboard({
      sensorIds: [1, 2],
      bufferCapacity: 500
    });

    // Ingest 600 live ticks for sensor 1 (overflowing 500 capacity)
    for (let i = 0; i < 600; i++) {
      dashboard.pushLiveSample(1, 1000 + i, 25.0 + i);
    }

    const sensor1Buffer = dashboard.getLiveBuffer(1);
    expect(sensor1Buffer).toBeDefined();
    expect(sensor1Buffer!.length).toBe(500);
    expect(sensor1Buffer!.capacity).toBe(500);
    expect(sensor1Buffer!.getOldest()?.timestampMs).toBe(1100);
    expect(sensor1Buffer!.getNewest()?.timestampMs).toBe(1599);

    // Contiguous export
    const contiguous = sensor1Buffer!.exportContiguous();
    expect(contiguous.length).toBe(500);
    expect(contiguous.timestamps[0]).toBe(1100);
    expect(contiguous.timestamps[499]).toBe(1599);
  });
});
