import { describe, it, expect, vi, beforeEach } from 'vitest';
import { useTelemetryStream } from '../src/composables/useTelemetryStream';
import { tableFromArrays, tableToIPC } from '@uwdata/flechette';

describe('useTelemetryStream composable', () => {
  beforeEach(() => {
    vi.restoreAllMocks();
  });

  it('decodes Apache Arrow IPC stream zero-copy into typed arrays', async () => {
    // Prepare synthetic Arrow IPC buffer
    const mockTable = tableFromArrays({
      sensor_id: new Uint32Array([1, 1, 1, 1]),
      ts_ms: new Float64Array([1000.0, 1015.0, 1045.0, 1059.0]),
      val: new Float64Array([20.0, 12.0, 48.0, 21.0])
    });
    const ipcBuffer = tableToIPC(mockTable, { format: 'stream' });

    globalThis.fetch = vi.fn().mockResolvedValue({
      ok: true,
      headers: new Headers({
        'Content-Type': 'application/vnd.apache.arrow.stream'
      }),
      arrayBuffer: async () => ipcBuffer!.buffer
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
    // 2 sensors, 4 points each (total 8 points)
    const mockTable = tableFromArrays({
      sensor_id: new Uint32Array([1, 1, 1, 1, 2, 2, 2, 2]),
      ts_ms: new Float64Array([100, 200, 300, 400, 100, 200, 300, 400]),
      val: new Float64Array([10.0, 15.0, 8.0, 12.0, 50.0, 55.0, 48.0, 52.0])
    });
    const ipcBuffer = tableToIPC(mockTable, { format: 'stream' });

    globalThis.fetch = vi.fn().mockResolvedValue({
      ok: true,
      headers: new Headers({
        'Content-Type': 'application/vnd.apache.arrow.stream'
      }),
      arrayBuffer: async () => ipcBuffer!.buffer
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

    // Verify s1 and s2 share underlying buffer without heap duplication (zero-copy)
    expect(s1.tsMs.buffer).toBe(batch.tsMs.buffer);
    expect(s2.tsMs.buffer).toBe(batch.tsMs.buffer);
    expect(s1.val.buffer).toBe(batch.val.buffer);

    // Values match
    expect(s1.val[0]).toBe(10.0);
    expect(s2.val[0]).toBe(50.0);
    expect(s1.tsMs[3]).toBe(400);
    expect(s2.tsMs[3]).toBe(400);
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

  it('detects and handles truncated/corrupted IPC stream buffers', async () => {
    // Truncated buffer (less than IPC minimum header)
    const corruptBuffer = new Uint8Array([0x01, 0x02, 0x03]).buffer;

    globalThis.fetch = vi.fn().mockResolvedValue({
      ok: true,
      headers: new Headers({ 'Content-Type': 'application/vnd.apache.arrow.stream' }),
      arrayBuffer: async () => corruptBuffer
    });

    const { fetchAm4Arrow, error } = useTelemetryStream();

    await expect(fetchAm4Arrow({ sensorId: 1, lbSec: 0, ubSec: 60, width: 800 })).rejects.toThrow();
    expect(error.value).not.toBeNull();
  });
});
