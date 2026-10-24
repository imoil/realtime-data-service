import { describe, it, expect, vi, beforeEach } from 'vitest';
import { mount } from '@vue/test-utils';
import TelemetryChart from '../src/components/TelemetryChart.vue';
import { tableFromArrays, tableToIPC } from '@uwdata/flechette';

// Mock lightningChart
vi.mock('@arction/lcjs', () => {
  const dummySeries = {
    clear: vi.fn(),
    appendSamples: vi.fn(),
    add: vi.fn()
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
    PointLineAreaSeries: {}
  };
});

describe('TelemetryChart.vue', () => {
  beforeEach(() => {
    vi.restoreAllMocks();
  });

  it('mounts and renders chart header and walking skeleton badge', async () => {
    const mockTable = tableFromArrays({
      sensor_id: new Uint32Array([1, 1]),
      ts_ms: new Float64Array([1000.0, 2000.0]),
      val: new Float64Array([25.0, 30.0])
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
        sensorId: 1,
        lbSec: 1729728000,
        ubSec: 1729728060,
        width: 1920
      }
    });

    expect(wrapper.text()).toContain('Sensor 1 Telemetry');
    expect(wrapper.text()).toContain('Walking Skeleton');
    expect(wrapper.find('button.btn-primary').text()).toBe('Refresh');
  });
});
