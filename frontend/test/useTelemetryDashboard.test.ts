import { describe, it, expect, vi, beforeEach } from 'vitest';
import { useTelemetryDashboard } from '../src/composables/useTelemetryDashboard';
import { tableFromArrays, tableToIPC } from '@uwdata/flechette';

describe('useTelemetryDashboard headless composable', () => {
  beforeEach(() => {
    vi.restoreAllMocks();

    const mockWs = {
      readyState: 1,
      send: vi.fn(),
      close: vi.fn(),
      addEventListener: vi.fn(),
      removeEventListener: vi.fn()
    };
    // @ts-expect-error mock WebSocket in test
    globalThis.WebSocket = vi.fn().mockImplementation(() => mockWs);
  });

  it('initializes with default 30-day macro state', () => {
    const dashboard = useTelemetryDashboard({
      sensorId: 1,
      lbSec: 1729728000,
      ubSec: 1729728000 + (30 * 86400)
    });

    expect(dashboard.isMacroRange.value).toBe(true);
    expect(dashboard.isMicroZoom.value).toBe(false);
    expect(dashboard.activeSensors.value).toEqual([1]);
    expect(dashboard.chartTitle.value).toBe('Sensor 1 Telemetry');
  });

  it('updates semantic zoom range with 120ms debounce and notifies historical data callback', async () => {
    vi.useFakeTimers();

    const mockTable = tableFromArrays({
      sensor_id: new Uint32Array([1, 1]),
      ts_ms: new Float64Array([1729728000000, 1729728001000]),
      val: new Float64Array([25.0, 30.0])
    });
    const ipcBuffer = tableToIPC(mockTable, { format: 'stream' })!;

    globalThis.fetch = vi.fn().mockResolvedValue({
      ok: true,
      headers: new Headers({
        'Content-Type': 'application/vnd.apache.arrow.stream'
      }),
      arrayBuffer: async () => ipcBuffer.buffer
    });

    const dashboard = useTelemetryDashboard({
      sensorIds: [1, 2],
      lbSec: 1729728000,
      ubSec: 1729728000 + (30 * 86400)
    });

    let receivedSensorCount = 0;
    dashboard.onHistoricalData((decomposed) => {
      receivedSensorCount = decomposed.size;
    });

    // Zoom to 2-hour micro range
    const zoomStartMs = 1729728000 * 1000;
    const zoomEndMs = (1729728000 + (2 * 3600)) * 1000;

    dashboard.zoomTo(zoomStartMs, zoomEndMs);

    // Before debounce
    expect(dashboard.currentUbSec.value).toBe(1729728000 + (30 * 86400));

    // Advance past 120ms debounce and flush asynchronous tasks
    await vi.advanceTimersByTimeAsync(130);

    expect(dashboard.currentLbSec.value).toBe(1729728000);
    expect(dashboard.currentUbSec.value).toBe(1729728000 + (2 * 3600));
    expect(dashboard.isMicroZoom.value).toBe(true);
    expect(receivedSensorCount).toBeGreaterThanOrEqual(1);

    vi.useRealTimers();
  });

  it('resets to initial 30-day macro range', () => {
    const baseLb = 1729728000;
    const baseUb = baseLb + (30 * 86400);

    const dashboard = useTelemetryDashboard({
      sensorIds: [1, 2, 3],
      lbSec: baseLb,
      ubSec: baseUb
    });

    // Simulate zoom in
    dashboard.currentLbSec.value = baseLb + 1000;
    dashboard.currentUbSec.value = baseLb + 5000;
    expect(dashboard.isMacroRange.value).toBe(false);

    // Reset view
    dashboard.resetToMacro();

    expect(dashboard.currentLbSec.value).toBe(baseLb);
    expect(dashboard.currentUbSec.value).toBe(baseUb);
    expect(dashboard.isMacroRange.value).toBe(true);
  });

  it('toggles live WebSocket streaming tail', () => {
    const dashboard = useTelemetryDashboard({ sensorId: 1 });

    expect(dashboard.isLiveEnabled.value).toBe(true);

    dashboard.toggleLiveTail();
    expect(dashboard.isLiveEnabled.value).toBe(false);

    dashboard.toggleLiveTail();
    expect(dashboard.isLiveEnabled.value).toBe(true);
  });
});
