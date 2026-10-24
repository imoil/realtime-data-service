<template>
  <div class="telemetry-chart-wrapper">
    <div class="chart-header">
      <div class="title-section">
        <h3>{{ chartTitle }}</h3>
        <span class="badge" :class="isMacroRange ? 'badge-macro' : (isMicroZoom ? 'badge-micro' : 'badge-intermediate')">
          {{ isMacroRange ? '30-Day Macro (1h Rollup)' : (isMicroZoom ? 'Micro-Zoom (Iceberg Raw)' : 'Intermediate (1m Rollup)') }}
        </span>
        <span class="live-badge" :class="isConnected ? 'live-online' : 'live-offline'">
          ● Live Tail: {{ isConnected ? 'Connected' : 'Connecting...' }}
        </span>
      </div>

      <div class="stats-section">
        <span class="stat-item">Active Sensors: <strong>{{ activeSensorsCount }}</strong></span>
        <span class="stat-item">Live Ticks: <strong class="live-counter">{{ tickCount }}</strong></span>
        <span class="stat-item">
          Window: <strong>{{ formatDuration(currentUbSec - currentLbSec) }}</strong>
        </span>
        <span v-if="isLoading" class="loading-text">Streaming Arrow IPC...</span>
      </div>

      <div class="actions-section">
        <button class="btn btn-secondary" :class="{ 'btn-active': isLiveEnabled }" @click="toggleLiveTail">
          {{ isLiveEnabled ? 'Pause Live' : 'Resume Live' }}
        </button>
        <button class="btn btn-primary" :disabled="isLoading" @click="resetToFullRange">
          Reset 30-Day View
        </button>
        <button class="btn btn-secondary" :disabled="isLoading" @click="refreshData">
          {{ isLoading ? 'Loading...' : 'Refresh' }}
        </button>
      </div>
    </div>

    <div v-if="error" class="error-banner">
      Streaming Error: {{ error }}
    </div>

    <!-- Main High-Resolution Semantic Zoom Chart -->
    <div ref="chartContainer" class="chart-container"></div>

    <!-- ZoomBandChart: 30-Day Global Overview Outline Mini-Map -->
    <div class="zoomband-wrapper">
      <div class="zoomband-header">
        <span>30-Day Global Overview Mini-Map (ZoomBandChart)</span>
        <span class="zoomband-hint">Drag or resize the selection band to navigate semantic zoom (120ms debounce)</span>
      </div>
      <div ref="zoomBandContainer" class="zoomband-container"></div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted, onUnmounted, watch } from 'vue';
import { lightningChart, ChartXY, PointLineAreaSeries, ColorHEX, SolidFill } from '@arction/lcjs';
import { useTelemetryStream } from '../composables/useTelemetryStream';
import { useLiveTelemetryTail } from '../composables/useLiveTelemetryTail';

const props = withDefaults(
  defineProps<{
    sensorId?: number;
    sensorIds?: number[];
    lbSec?: number;
    ubSec?: number;
    width?: number;
  }>(),
  {
    sensorId: 1,
    sensorIds: () => [1],
    lbSec: 1729728000,
    ubSec: 1729728000 + (30 * 86400),
    width: 1920
  }
);

const chartContainer = ref<HTMLDivElement | null>(null);
const zoomBandContainer = ref<HTMLDivElement | null>(null);

let chart: ChartXY | null = null;
let zoomBandChart: any = null;
const seriesMap = new Map<number, PointLineAreaSeries>();
const zoomBandSeriesMap = new Map<number, PointLineAreaSeries>();

const currentLbSec = ref(props.lbSec);
const currentUbSec = ref(props.ubSec);

const SENSOR_PALETTE = [
  '#58a6ff', '#3fb950', '#d29922', '#f85149', '#bc8cff',
  '#f0883e', '#79c0ff', '#56d364', '#e3b341', '#ff7b72'
];

const targetSensorIds = computed(() => {
  if (props.sensorIds && props.sensorIds.length > 0) {
    return props.sensorIds;
  }
  return [props.sensorId];
});

const isMacroRange = computed(() => {
  return (currentUbSec.value - currentLbSec.value) > (7 * 86400);
});

const isMicroZoom = computed(() => {
  return (currentUbSec.value - currentLbSec.value) <= (6 * 3600);
});

const activeSensorsCount = computed(() => targetSensorIds.value.length);

const chartTitle = computed(() => {
  if (targetSensorIds.value.length === 1) {
    return `Sensor ${targetSensorIds.value[0]} Telemetry`;
  }
  return `Synchronized Telemetry (${targetSensorIds.value.length} Sensors)`;
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

function formatDuration(sec: number): string {
  if (sec >= 86400) {
    const d = Math.round(sec / 86400);
    return `${d} Day${d > 1 ? 's' : ''}`;
  }
  if (sec >= 3600) {
    const h = Math.round(sec / 3600);
    return `${h} Hour${h > 1 ? 's' : ''}`;
  }
  if (sec >= 60) {
    const m = Math.round(sec / 60);
    return `${m} Min`;
  }
  return `${sec} Sec`;
}

function getOrCreateSeries(sensorId: number): PointLineAreaSeries | null {
  if (!chart) return null;
  if (seriesMap.has(sensorId)) {
    return seriesMap.get(sensorId)!;
  }

  const s = chart.addPointLineAreaSeries();
  if (!s) return null;

  if (typeof (s as any).setName === 'function') {
    (s as any).setName(`Sensor ${sensorId}`);
  }

  const colorHex = SENSOR_PALETTE[sensorId % SENSOR_PALETTE.length];
  try {
    if (typeof (s as any).setStrokeStyle === 'function') {
      (s as any).setStrokeStyle((stroke: any) => stroke.setFillStyle(new SolidFill({ color: ColorHEX(colorHex) })));
    }
  } catch {
    // optional styling fallback
  }

  seriesMap.set(sensorId, s);
  return s;
}

function getOrCreateZoomBandSeries(sensorId: number): PointLineAreaSeries | null {
  if (!zoomBandChart) return null;
  if (zoomBandSeriesMap.has(sensorId)) {
    return zoomBandSeriesMap.get(sensorId)!;
  }

  let s: PointLineAreaSeries | null = null;
  if (typeof zoomBandChart.addPointLineAreaSeries === 'function') {
    s = zoomBandChart.addPointLineAreaSeries();
  }
  if (!s) return null;

  const colorHex = SENSOR_PALETTE[sensorId % SENSOR_PALETTE.length];
  try {
    if (typeof (s as any).setStrokeStyle === 'function') {
      (s as any).setStrokeStyle((stroke: any) => stroke.setFillStyle(new SolidFill({ color: ColorHEX(colorHex) })));
    }
  } catch {
    // optional styling fallback
  }

  zoomBandSeriesMap.set(sensorId, s);
  return s;
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

/**
 * Loads high-resolution semantic zoom data for the active time window.
 */
async function loadData(lbSec = currentLbSec.value, ubSec = currentUbSec.value) {
  try {
    isProgrammaticUpdate.value = true;
    const batch = await fetchAm4Arrow({
      sensorIds: targetSensorIds.value,
      lbSec,
      ubSec,
      width: props.width
    });

    if (chart && batch.length > 0) {
      const decomposed = decomposeBySensor(batch);

      // Clean up series for removed sensors
      const currentActiveSensors = new Set(targetSensorIds.value);
      for (const [sId, sInstance] of seriesMap.entries()) {
        if (!currentActiveSensors.has(sId)) {
          if (typeof sInstance.dispose === 'function') {
            sInstance.dispose();
          }
          seriesMap.delete(sId);
        }
      }

      // Inject zero-copy slices into each sensor's main series
      for (const [sensorId, sensorData] of decomposed.entries()) {
        const s = getOrCreateSeries(sensorId);
        if (s) {
          s.clear();
          const seriesAny = s as unknown as { appendSamples?: (args: unknown) => void; add: (pts: unknown[]) => void };
          if (typeof seriesAny.appendSamples === 'function') {
            seriesAny.appendSamples({
              xValues: sensorData.tsMs,
              yValues: sensorData.val
            });
          } else {
            const points = [];
            for (let i = 0; i < sensorData.length; i++) {
              points.push({ x: sensorData.tsMs[i], y: sensorData.val[i] });
            }
            seriesAny.add(points);
          }
        }
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
 * Loads 30-day macro overview outline into the ZoomBandChart.
 */
async function loadOverviewData() {
  try {
    const macroBatch = await fetchAm4Arrow({
      sensorIds: targetSensorIds.value,
      lbSec: props.lbSec,
      ubSec: props.ubSec,
      width: 1000
    });

    if (zoomBandChart && macroBatch.length > 0) {
      const decomposed = decomposeBySensor(macroBatch);
      for (const [sensorId, sensorData] of decomposed.entries()) {
        const s = getOrCreateZoomBandSeries(sensorId);
        if (s) {
          s.clear();
          const seriesAny = s as unknown as { appendSamples?: (args: unknown) => void; add: (pts: unknown[]) => void };
          if (typeof seriesAny.appendSamples === 'function') {
            seriesAny.appendSamples({
              xValues: sensorData.tsMs,
              yValues: sensorData.val
            });
          } else {
            const points = [];
            for (let i = 0; i < sensorData.length; i++) {
              points.push({ x: sensorData.tsMs[i], y: sensorData.val[i] });
            }
            seriesAny.add(points);
          }
        }
      }
    }
  } catch (err: unknown) {
    if ((err as Error)?.name !== 'AbortError') {
      console.warn('Failed to load ZoomBand overview outline:', err);
    }
  }
}

/**
 * Debounced semantic zoom navigator triggered by ZoomBandChart dragging or resizing.
 */
const debouncedZoomNavigate = debounce((startMs: number, endMs: number) => {
  if (isProgrammaticUpdate.value) return;

  const newLbSec = Math.floor(startMs / 1000);
  const newUbSec = Math.ceil(endMs / 1000);

  if (newUbSec > newLbSec) {
    currentLbSec.value = newLbSec;
    currentUbSec.value = newUbSec;
    loadData(newLbSec, newUbSec);
  }
}, 120);

function handleZoomBandIntervalChange(startMs: number, endMs: number) {
  debouncedZoomNavigate(startMs, endMs);
}

function resetToFullRange() {
  currentLbSec.value = props.lbSec;
  currentUbSec.value = props.ubSec;
  if (zoomBandChart && typeof zoomBandChart.setInterval === 'function') {
    isProgrammaticUpdate.value = true;
    try {
      zoomBandChart.setInterval(props.lbSec * 1000, props.ubSec * 1000);
    } finally {
      isProgrammaticUpdate.value = false;
    }
  }
  loadData(props.lbSec, props.ubSec);
}

function refreshData() {
  loadData(currentLbSec.value, currentUbSec.value);
}

function toggleLiveTail() {
  if (isLiveEnabled.value) {
    disconnectLiveTail();
  } else {
    connectLiveTail();
    subscribeLiveTail(targetSensorIds.value);
  }
}

// Live tail listener: real-time sample appending without full series re-render
onTick((tick) => {
  const s = getOrCreateSeries(tick.sensorId);
  if (s) {
    const seriesAny = s as unknown as { appendSamples?: (args: unknown) => void; add: (pts: unknown[]) => void };
    if (typeof seriesAny.appendSamples === 'function') {
      seriesAny.appendSamples({
        xValues: [tick.timestampMs],
        yValues: [tick.value]
      });
    } else {
      seriesAny.add([{ x: tick.timestampMs, y: tick.value }]);
    }
  }
});

onMounted(() => {
  if (chartContainer.value) {
    try {
      chart = lightningChart().ChartXY({
        container: chartContainer.value
      });
      chart.setTitle(chartTitle.value);

      // Create ZoomBandChart mini-map linked to the main chart
      if (zoomBandContainer.value) {
        if (typeof (chart as any).createZoomBandChart === 'function') {
          zoomBandChart = (chart as any).createZoomBandChart({
            container: zoomBandContainer.value
          });
        } else {
          try {
            zoomBandChart = (lightningChart() as any).ZoomBandChart?.({
              container: zoomBandContainer.value
            });
          } catch {
            // fallback
          }
        }

        if (zoomBandChart && typeof zoomBandChart.onIntervalChange === 'function') {
          zoomBandChart.onIntervalChange(handleZoomBandIntervalChange);
        }
      }

      loadOverviewData();
      loadData(currentLbSec.value, currentUbSec.value);
    } catch (e) {
      console.warn('WebGL / LightningChart initialization note:', e);
    }
  }

  // Connect live WebSocket tail and subscribe active sensors
  connectLiveTail();
  subscribeLiveTail(targetSensorIds.value);
});

onUnmounted(() => {
  disconnectLiveTail();
  if (zoomBandChart && typeof zoomBandChart.dispose === 'function') {
    zoomBandChart.dispose();
    zoomBandChart = null;
    zoomBandSeriesMap.clear();
  }
  if (chart) {
    chart.dispose();
    chart = null;
    seriesMap.clear();
  }
});

watch(
  () => [props.sensorIds, props.sensorId, props.lbSec, props.ubSec],
  () => {
    if (!isProgrammaticUpdate.value) {
      currentLbSec.value = props.lbSec;
      currentUbSec.value = props.ubSec;
      if (chart) {
        chart.setTitle(chartTitle.value);
      }
      loadOverviewData();
      loadData(props.lbSec, props.ubSec);
      subscribeLiveTail(targetSensorIds.value);
    }
  },
  { deep: true }
);

defineExpose({
  handleZoomBandIntervalChange,
  currentLbSec,
  currentUbSec,
  isProgrammaticUpdate,
  loadData,
  resetToFullRange
});
</script>

<style scoped>
.telemetry-chart-wrapper {
  display: flex;
  flex-direction: column;
  width: 100%;
  height: 100%;
  border-radius: 8px;
  background-color: #1a1e24;
  color: #e6edf3;
  padding: 16px;
  box-sizing: border-box;
}

.chart-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 12px;
}

.title-section {
  display: flex;
  align-items: center;
  gap: 12px;
}

.title-section h3 {
  margin: 0;
  font-size: 1.15rem;
}

.badge {
  color: #fff;
  padding: 2px 10px;
  border-radius: 12px;
  font-size: 0.75rem;
  font-weight: 600;
}

.badge-macro {
  background-color: #8957e5;
}

.badge-intermediate {
  background-color: #238636;
}

.badge-micro {
  background-color: #1f6feb;
}

.live-badge {
  font-size: 0.75rem;
  font-weight: 600;
  padding: 2px 10px;
  border-radius: 12px;
}

.live-online {
  background-color: rgba(35, 134, 54, 0.2);
  color: #3fb950;
  border: 1px solid #238636;
}

.live-offline {
  background-color: rgba(210, 153, 34, 0.2);
  color: #d29922;
  border: 1px solid #d29922;
}

.stats-section {
  display: flex;
  gap: 16px;
  font-size: 0.85rem;
  color: #8b949e;
}

.stat-item strong {
  color: #58a6ff;
}

.live-counter {
  color: #3fb950 !important;
}

.loading-text {
  color: #d29922;
  font-weight: 600;
}

.actions-section {
  display: flex;
  gap: 8px;
}

.btn {
  border: 1px solid rgba(240, 246, 252, 0.1);
  padding: 6px 14px;
  border-radius: 6px;
  cursor: pointer;
  font-weight: 500;
  font-size: 0.85rem;
}

.btn-primary {
  background-color: #238636;
  color: #fff;
}

.btn-secondary {
  background-color: #21262d;
  color: #c9d1d9;
}

.btn-active {
  border-color: #3fb950;
  color: #3fb950;
}

.btn:disabled {
  opacity: 0.6;
  cursor: not-allowed;
}

.error-banner {
  background-color: #f8514922;
  color: #f85149;
  border: 1px solid #f85149;
  padding: 8px 12px;
  border-radius: 6px;
  margin-bottom: 8px;
  font-size: 0.85rem;
}

.chart-container {
  flex: 1;
  min-height: 440px;
  width: 100%;
  border-radius: 4px;
  background-color: #0d1117;
}

.zoomband-wrapper {
  margin-top: 12px;
  background-color: #161b22;
  border-radius: 6px;
  padding: 8px 12px;
  border: 1px solid rgba(240, 246, 252, 0.1);
}

.zoomband-header {
  display: flex;
  justify-content: space-between;
  font-size: 0.75rem;
  color: #8b949e;
  margin-bottom: 6px;
}

.zoomband-hint {
  color: #58a6ff;
}

.zoomband-container {
  height: 90px;
  width: 100%;
  border-radius: 4px;
  background-color: #0d1117;
}
</style>
