import { describe, it, expect, vi, beforeEach } from 'vitest';
import { useTelemetryStream, hasValidArrowEos } from '../src/composables/useTelemetryStream';
import { tableFromArrays, tableToIPC } from '@uwdata/flechette';

describe('useTelemetryStream composable', () => {
  beforeEach(() => {
    vi.restoreAllMocks();
  });

  it('validates Arrow EOS marker (0xFFFFFFFF) in stream buffer', () => {
    const mockTable = tableFromArrays({
      a: [1, 2]
    });
    const validIpc = tableToIPC(mockTable, { format: 'stream' })!;
    expect(hasValidArrowEos(validIpc.buffer)).toBe(true);

    // Truncated buffer missing EOS bytes
    const truncatedBuffer = validIpc.buffer.slice(0, validIpc.buffer.byteLength - 8);
    const corruptBuffer = new Uint8Array([0x01, 0x02, 0x03, 0x04]).buffer;
    expect(hasValidArrowEos(truncatedBuffer)).toBe(false);
    expect(hasValidArrowEos(corruptBuffer)).toBe(false);

    // Empty or short buffer
    expect(hasValidArrowEos(new ArrayBuffer(2))).toBe(false);
  });

  it('decodes Apache Arrow IPC stream zero-copy into typed arrays', async () => {
    const mockTable = tableFromArrays({
      sensor_id: new Uint32Array([1, 1, 1, 1]),
      ts_ms: new Float64Array([1000.0, 1015.0, 1045.0, 1059.0]),
      val: new Float64Array([20.0, 12.0, 48.0, 21.0])
    });
    const ipcBuffer = tableToIPC(mockTable, { format: 'stream' })!;

    globalThis.fetch = vi.fn().mockResolvedValue({
      ok: true,
      headers: new Headers({
        'Content-Type': 'application/vnd.apache.arrow.stream'
      }),
      arrayBuffer: async () => ipcBuffer.buffer
    });

    const { fetchAm4Arrow, isLoading, error } = useTelemetryStream();

    const batch = await fetchAm4Arrow({
      sensorId: 1,
      lbSec: 1,
      ubSec: 2,
      width: 1920
    });

    expect(isLoading.value).toBe(false);
    expect(error.value).toBeNull();
    expect(batch.length).toBe(4);
    expect(batch.sensorId).toBeInstanceOf(Uint32Array);
    expect(batch.tsMs).toBeInstanceOf(Float64Array);
    expect(batch.val).toBeInstanceOf(Float64Array);

    expect(batch.sensorId[0]).toBe(1);
    expect(batch.tsMs[0]).toBe(1000.0);
    expect(batch.val[1]).toBe(12.0);
    expect(batch.val[2]).toBe(48.0);
  });

  it('decomposes multi-sensor Arrow streams into zero-copy subarray views per sensor', async () => {
    const mockTable = tableFromArrays({
      sensor_id: new Uint32Array([1, 1, 1, 1, 2, 2, 2, 2]),
      ts_ms: new Float64Array([100, 200, 300, 400, 100, 200, 300, 400]),
      val: new Float64Array([10.0, 15.0, 8.0, 12.0, 50.0, 55.0, 48.0, 52.0])
    });
    const ipcBuffer = tableToIPC(mockTable, { format: 'stream' })!;

    globalThis.fetch = vi.fn().mockResolvedValue({
      ok: true,
      headers: new Headers({
        'Content-Type': 'application/vnd.apache.arrow.stream'
      }),
      arrayBuffer: async () => ipcBuffer.buffer
    });

    const { fetchAm4Arrow, decomposeBySensor } = useTelemetryStream();

    const batch = await fetchAm4Arrow({
      sensorIds: [1, 2],
      lbSec: 0,
      ubSec: 1000,
      width: 1920
    });

    expect(batch.length).toBe(8);

    const seriesMap = decomposeBySensor(batch);
    expect(seriesMap.size).toBe(2);

    const s1 = seriesMap.get(1)!;
    const s2 = seriesMap.get(2)!;

    expect(s1.length).toBe(4);
    expect(s2.length).toBe(4);

    expect(s1.tsMs.buffer).toBe(batch.tsMs.buffer);
    expect(s2.tsMs.buffer).toBe(batch.tsMs.buffer);
    expect(s1.val.buffer).toBe(batch.val.buffer);

    expect(s1.val[0]).toBe(10.0);
    expect(s2.val[0]).toBe(50.0);
  });

  it('cancels pending requests with AbortController when a new request is initiated', async () => {
    let aborted = false;
    globalThis.fetch = vi.fn().mockImplementation((_url: string, init?: RequestInit) => {
      const signal = init?.signal as AbortSignal;
      signal?.addEventListener('abort', () => {
        aborted = true;
      });
      return new Promise(() => {}); // never resolves
    });

    const { fetchAm4Arrow } = useTelemetryStream();

    // Fire first request
    fetchAm4Arrow({ sensorId: 1, lbSec: 0, ubSec: 60, width: 800 }).catch(() => {});

    // Fire second request immediately
    fetchAm4Arrow({ sensorId: 1, lbSec: 60, ubSec: 120, width: 800 }).catch(() => {});

    expect(aborted).toBe(true);
  });

  it('triggers exponential backoff retry on transient stream errors and succeeds', async () => {
    const mockTable = tableFromArrays({
      sensor_id: new Uint32Array([1]),
      ts_ms: new Float64Array([1000]),
      val: new Float64Array([25.0])
    });
    const validBuffer = tableToIPC(mockTable, { format: 'stream' })!.buffer;

    let callCount = 0;
    globalThis.fetch = vi.fn().mockImplementation(async () => {
      callCount++;
      if (callCount === 1) {
        // First attempt fails with malformed/truncated stream
        return {
          ok: true,
          arrayBuffer: async () => new Uint8Array([0x01, 0x02, 0x03, 0x04]).buffer
        };
      }
      // Second attempt succeeds
      return {
        ok: true,
        arrayBuffer: async () => validBuffer
      };
    });

    const { fetchAm4Arrow, retryCount, error } = useTelemetryStream();

    const batch = await fetchAm4Arrow({
      sensorId: 1,
      lbSec: 0,
      ubSec: 60,
      maxRetries: 2,
      initialDelayMs: 10,
      backoffMultiplier: 2
    });

    expect(callCount).toBe(2);
    expect(retryCount.value).toBe(1);
    expect(error.value).toBeNull();
    expect(batch.length).toBe(1);
  });

  it('fails and sets error state after exhausting max retries', async () => {
    let callCount = 0;
    globalThis.fetch = vi.fn().mockImplementation(async () => {
      callCount++;
      return {
        ok: false,
        status: 503,
        statusText: 'Service Unavailable'
      };
    });

    const { fetchAm4Arrow, retryCount, error } = useTelemetryStream();

    await expect(fetchAm4Arrow({
      sensorId: 1,
      lbSec: 0,
      ubSec: 60,
      maxRetries: 2,
      initialDelayMs: 10
    })).rejects.toThrow('HTTP error 503: Service Unavailable');

    expect(callCount).toBe(3); // Initial + 2 retries
    expect(retryCount.value).toBe(2);
    expect(error.value).toContain('HTTP error 503');
  });
});
