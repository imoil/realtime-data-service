import { describe, it, expect, vi, beforeEach } from 'vitest';
import { mount, flushPromises } from '@vue/test-utils';
import TelemetryChart from '../src/components/TelemetryChart.vue';
import { tableFromArrays, tableToIPC } from '@uwdata/flechette';

const { dummySeries, dummyZoomBandChart, dummyChart, getRegisteredIntervalCallback, setRegisteredIntervalCallback } = vi.hoisted(() => {
  let intervalCb: ((start: number, end: number) => void) | null = null;
  const dummySeries = {
    clear: vi.fn(),
    appendSamples: vi.fn(),
    add: vi.fn(),
    setName: vi.fn(),
    setStrokeStyle: vi.fn(),
    dispose: vi.fn()
  };
  const dummyZoomBandChart = {
    addPointLineAreaSeries: vi.fn().mockReturnValue(dummySeries),
    onIntervalChange: vi.fn((cb: (start: number, end: number) => void) => {
      intervalCb = cb;
    }),
    setInterval: vi.fn(),
    dispose: vi.fn()
  };
  const dummyChart = {
    setTitle: vi.fn(),
    addPointLineAreaSeries: vi.fn().mockReturnValue(dummySeries),
    createZoomBandChart: vi.fn().mockReturnValue(dummyZoomBandChart),
    dispose: vi.fn()
  };
  return {
    dummySeries,
    dummyZoomBandChart,
    dummyChart,
    getRegisteredIntervalCallback: () => intervalCb,
    setRegisteredIntervalCallback: (cb: any) => { intervalCb = cb; }
  };
});

vi.mock('@arction/lcjs', () => {
  return {
    lightningChart: () => ({
      ChartXY: () => dummyChart,
      ZoomBandChart: () => dummyZoomBandChart
    }),
    PointLineAreaSeries: {},
    ColorHEX: (c: string) => c,
    SolidFill: class {
      constructor(public options: any) {}
    }
  };
});

describe('TelemetryChart.vue', () => {
  beforeEach(() => {
    setRegisteredIntervalCallback(null);

    dummyZoomBandChart.onIntervalChange.mockImplementation((cb: (start: number, end: number) => void) => {
      setRegisteredIntervalCallback(cb);
    });
    dummyChart.createZoomBandChart.mockReturnValue(dummyZoomBandChart);
    dummyChart.addPointLineAreaSeries.mockReturnValue(dummySeries);
    dummyZoomBandChart.addPointLineAreaSeries.mockReturnValue(dummySeries);

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

  it('mounts and renders multi-sensor header and ZoomBandChart mini-map', async () => {
    const mockTable = tableFromArrays({
      sensor_id: new Uint32Array([1, 1, 2, 2]),
      ts_ms: new Float64Array([1000.0, 2000.0, 1000.0, 2000.0]),
      val: new Float64Array([25.0, 30.0, 45.0, 50.0])
    });
    const ipcBuffer = tableToIPC(mockTable, { format: 'stream' })!;

    globalThis.fetch = vi.fn().mockResolvedValue({
      ok: true,
      headers: new Headers({
        'Content-Type': 'application/vnd.apache.arrow.stream'
      }),
      arrayBuffer: async () => ipcBuffer.buffer
    });

    const wrapper = mount(TelemetryChart as any, {
      props: {
        sensorIds: [1, 2],
        lbSec: 1729728000,
        ubSec: 1729728000 + (30 * 86400),
        width: 1920
      }
    });

    expect(wrapper.text()).toContain('Synchronized Telemetry (2 Sensors)');
    expect(wrapper.text()).toContain('30-Day Macro (1h Rollup)');
    expect(wrapper.text()).toContain('30-Day Global Overview Mini-Map');
    expect(wrapper.find('.zoomband-container').exists()).toBe(true);
    expect(wrapper.find('button.btn-primary').text()).toBe('Reset 30-Day View');
  });

  it('updates semantic zoom range when ZoomBandChart interval changes with 120ms debounce', async () => {
    vi.useFakeTimers();

    const mockTable = tableFromArrays({
      sensor_id: new Uint32Array([1]),
      ts_ms: new Float64Array([1729728000000]),
      val: new Float64Array([30.0])
    });
    const ipcBuffer = tableToIPC(mockTable, { format: 'stream' })!;

    globalThis.fetch = vi.fn().mockResolvedValue({
      ok: true,
      headers: new Headers({
        'Content-Type': 'application/vnd.apache.arrow.stream'
      }),
      arrayBuffer: async () => ipcBuffer.buffer
    });

    const wrapper = mount(TelemetryChart as any, {
      props: {
        sensorIds: [1],
        lbSec: 1729728000,
        ubSec: 1729728000 + (30 * 86400),
        width: 1920
      }
    });

    // Flush initial load in onMounted
    await flushPromises();

    const callback = getRegisteredIntervalCallback();
    expect(callback).not.toBeNull();

    // User drags ZoomBandChart to 6-hour range
    const zoomStartMs = 1729728000 * 1000;
    const zoomEndMs = (1729728000 + (6 * 3600)) * 1000;

    callback!(zoomStartMs, zoomEndMs);

    // Before debounce timer fires
    expect((wrapper.vm as any).currentUbSec).toBe(1729728000 + (30 * 86400));

    // Advance timers past 120ms debounce
    vi.advanceTimersByTime(130);

    expect((wrapper.vm as any).currentLbSec).toBe(1729728000);
    expect((wrapper.vm as any).currentUbSec).toBe(1729728000 + (6 * 3600));

    vi.useRealTimers();
  });

  it('guards against circular re-query storms using isProgrammaticUpdate', async () => {
    const mockTable = tableFromArrays({
      sensor_id: new Uint32Array([1]),
      ts_ms: new Float64Array([1000]),
      val: new Float64Array([25.0])
    });
    const ipcBuffer = tableToIPC(mockTable, { format: 'stream' })!;

    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      headers: new Headers({ 'Content-Type': 'application/vnd.apache.arrow.stream' }),
      arrayBuffer: async () => ipcBuffer.buffer
    });
    globalThis.fetch = fetchMock;

    const wrapper = mount(TelemetryChart as any, {
      props: {
        sensorIds: [1],
        lbSec: 1000,
        ubSec: 2000
      }
    });

    await flushPromises();

    // Simulate programmatic update in progress
    (wrapper.vm as any).isProgrammaticUpdate = true;
    const initialCallCount = fetchMock.mock.calls.length;

    (wrapper.vm as any).handleZoomBandIntervalChange(1500 * 1000, 1800 * 1000);

    // Should not trigger fetch because isProgrammaticUpdate is true
    expect(fetchMock.mock.calls.length).toBe(initialCallCount);
  });
});
