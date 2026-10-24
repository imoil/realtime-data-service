import { describe, it, expect, vi, beforeEach } from 'vitest';
import { mount } from '@vue/test-utils';
import TelemetryChart from '../src/components/TelemetryChart.vue';
import { tableFromArrays, tableToIPC } from '@uwdata/flechette';

// Mock lightningChart
vi.mock('@arction/lcjs', () => {
  const dummySeries = {
    clear: vi.fn(),
    appendSamples: vi.fn(),
    add: vi.fn(),
    setName: vi.fn(),
    setStrokeStyle: vi.fn(),
    dispose: vi.fn()
  };
  const dummyChart = {
    setTitle: vi.fn(),
    addPointLineAreaSeries: vi.fn().mockReturnValue(dummySeries),
    dispose: vi.fn()
  };
  return {
    lightningChart: () => ({
      ChartXY: () => dummyChart
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
    vi.restoreAllMocks();
  });

  it('mounts and renders multi-sensor 30-day macro badge and header', async () => {
    const mockTable = tableFromArrays({
      sensor_id: new Uint32Array([1, 1, 2, 2]),
      ts_ms: new Float64Array([1000.0, 2000.0, 1000.0, 2000.0]),
      val: new Float64Array([25.0, 30.0, 45.0, 50.0])
    });
    const ipcBuffer = tableToIPC(mockTable, { format: 'stream' });

    globalThis.fetch = vi.fn().mockResolvedValue({
      ok: true,
      headers: new Headers({
        'Content-Type': 'application/vnd.apache.arrow.stream'
      }),
      arrayBuffer: async () => ipcBuffer!.buffer
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
    expect(wrapper.find('button.btn-primary').text()).toBe('Refresh');
  });
});
