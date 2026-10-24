import { ref, computed } from 'vue';
import { useTelemetryStream, type SensorSeries } from './useTelemetryStream';
import { useLiveTelemetryTail } from './useLiveTelemetryTail';
import {
  CircularTelemetryBuffer,
  CircularTelemetryBufferPool
} from '../utils/CircularTelemetryBuffer';

export interface DashboardOptions {
  sensorId?: number;
  sensorIds?: number[];
  lbSec?: number;
  ubSec?: number;
  width?: number;
  bufferCapacity?: number;
  maxBufferSensors?: number;
}

/**
 * 120ms debounce utility to coalesce rapid zoom/pan events
 * and prevent circular chart re-query storms.
 */
function debounce<T extends (...args: any[]) => void>(fn: T, delayMs = 120): (...args: Parameters<T>) => void {
  let timeoutId: ReturnType<typeof setTimeout> | null = null;
  return (...args: Parameters<T>) => {
    if (timeoutId) clearTimeout(timeoutId);
    timeoutId = setTimeout(() => {
      fn(...args);
    }, delayMs);
  };
}

export function useTelemetryDashboard(options: DashboardOptions = {}) {
  const defaultLb = options.lbSec ?? 1729728000;
  const defaultUb = options.ubSec ?? 1729728000 + (30 * 86400);

  const initialLbSec = ref(defaultLb);
  const initialUbSec = ref(defaultUb);
  const currentLbSec = ref(defaultLb);
  const currentUbSec = ref(defaultUb);

  const rawSensorIds = ref(options.sensorIds && options.sensorIds.length > 0 ? options.sensorIds : [options.sensorId ?? 1]);
  const width = ref(options.width ?? 1920);

  // Fixed-budget circular ring buffer pool for continuous 24/7 live monitoring
  const bufferPool = new CircularTelemetryBufferPool({
    maxSensors: options.maxBufferSensors ?? 50,
    capacityPerSensor: options.bufferCapacity ?? 10_000
  });

  const activeSensors = computed(() => {
    if (rawSensorIds.value && rawSensorIds.value.length > 0) {
      return rawSensorIds.value;
    }
    return [1];
  });

  const activeSensorsCount = computed(() => activeSensors.value.length);

  const isMacroRange = computed(() => {
    return (currentUbSec.value - currentLbSec.value) > (7 * 86400);
  });

  const isMicroZoom = computed(() => {
    return (currentUbSec.value - currentLbSec.value) <= (6 * 3600);
  });

  const chartTitle = computed(() => {
    if (activeSensors.value.length === 1) {
      return `Sensor ${activeSensors.value[0]} Telemetry`;
    }
    return `Synchronized Telemetry (${activeSensors.value.length} Sensors)`;
  });

  const {
    isLoading,
    error,
    isProgrammaticUpdate,
    decomposeBySensor,
    fetchAm4Arrow
  } = useTelemetryStream();

  const {
    isConnected,
    tickCount,
    isLiveEnabled,
    connect: connectLiveTail,
    disconnect: disconnectLiveTail,
    subscribe: subscribeLiveTail,
    onTick
  } = useLiveTelemetryTail();

  // Pipe incoming live WebSocket ticks automatically into the circular buffer pool
  onTick((tick) => {
    bufferPool.push(tick.sensorId, tick.timestampMs, tick.value);
  });

  // Callbacks for visual consumers
  const historicalDataCallbacks: Array<(data: Map<number, SensorSeries>) => void> = [];
  const overviewDataCallbacks: Array<(data: Map<number, SensorSeries>) => void> = [];

  function onHistoricalData(cb: (data: Map<number, SensorSeries>) => void) {
    historicalDataCallbacks.push(cb);
  }

  function onOverviewData(cb: (data: Map<number, SensorSeries>) => void) {
    overviewDataCallbacks.push(cb);
  }

  /**
   * Loads high-resolution semantic zoom data for the active time window.
   */
  async function loadHistoricalData(lbSec = currentLbSec.value, ubSec = currentUbSec.value) {
    try {
      isProgrammaticUpdate.value = true;
      const batch = await fetchAm4Arrow({
        sensorIds: activeSensors.value,
        lbSec,
        ubSec,
        width: width.value
      });

      if (batch.length > 0) {
        const decomposed = decomposeBySensor(batch);
        for (const cb of historicalDataCallbacks) {
          cb(decomposed);
        }
      }
    } catch (err: unknown) {
      if ((err as Error)?.name !== 'AbortError') {
        console.error('Failed to stream telemetry:', err);
      }
    } finally {
      isProgrammaticUpdate.value = false;
    }
  }

  /**
   * Loads 30-day macro overview outline into ZoomBandChart.
   */
  async function loadOverviewData() {
    try {
      const macroBatch = await fetchAm4Arrow({
        sensorIds: activeSensors.value,
        lbSec: initialLbSec.value,
        ubSec: initialUbSec.value,
        width: 1000
      });

      if (macroBatch.length > 0) {
        const decomposed = decomposeBySensor(macroBatch);
        for (const cb of overviewDataCallbacks) {
          cb(decomposed);
        }
      }
    } catch (err: unknown) {
      if ((err as Error)?.name !== 'AbortError') {
        console.warn('Failed to load ZoomBand overview outline:', err);
      }
    }
  }

  /**
   * 120ms debounced semantic zoom navigation from ZoomBandChart dragging or resizing.
   */
  const debouncedZoomNavigate = debounce((startMs: number, endMs: number) => {
    if (isProgrammaticUpdate.value) return;

    const newLbSec = Math.floor(startMs / 1000);
    const newUbSec = Math.ceil(endMs / 1000);

    if (newUbSec > newLbSec) {
      currentLbSec.value = newLbSec;
      currentUbSec.value = newUbSec;
      loadHistoricalData(newLbSec, newUbSec);
    }
  }, 120);

  function zoomTo(startMs: number, endMs: number) {
    debouncedZoomNavigate(startMs, endMs);
  }

  function resetToMacro() {
    currentLbSec.value = initialLbSec.value;
    currentUbSec.value = initialUbSec.value;
    loadHistoricalData(initialLbSec.value, initialUbSec.value);
  }

  function refresh() {
    loadHistoricalData(currentLbSec.value, currentUbSec.value);
  }

  function toggleLiveTail() {
    isLiveEnabled.value = !isLiveEnabled.value;
    if (isLiveEnabled.value) {
      connectLiveTail();
      subscribeLiveTail(activeSensors.value);
    } else {
      disconnectLiveTail();
    }
  }

  function updateSensors(sensorIds: number[]) {
    rawSensorIds.value = sensorIds;
    subscribeLiveTail(sensorIds);
    loadOverviewData();
    loadHistoricalData();
  }

  function pushLiveSample(sensorId: number, timestampMs: number, value: number): void {
    bufferPool.push(sensorId, timestampMs, value);
  }

  function getLiveBuffer(sensorId: number): CircularTelemetryBuffer {
    return bufferPool.getOrCreateBuffer(sensorId);
  }

  function mountDashboard() {
    loadOverviewData();
    loadHistoricalData();
    connectLiveTail();
    subscribeLiveTail(activeSensors.value);
  }

  function disposeDashboard() {
    disconnectLiveTail();
    historicalDataCallbacks.length = 0;
    overviewDataCallbacks.length = 0;
  }

  return {
    // State
    currentLbSec,
    currentUbSec,
    initialLbSec,
    initialUbSec,
    activeSensors,
    activeSensorsCount,
    isMacroRange,
    isMicroZoom,
    chartTitle,
    isLoading,
    error,
    isProgrammaticUpdate,
    isConnected,
    tickCount,
    isLiveEnabled,
    bufferPool,

    // Actions
    zoomTo,
    resetToMacro,
    refresh,
    toggleLiveTail,
    updateSensors,
    loadHistoricalData,
    loadOverviewData,
    mountDashboard,
    disposeDashboard,
    pushLiveSample,
    getLiveBuffer,

    // Subscriptions
    onHistoricalData,
    onOverviewData,
    onTick
  };
}
