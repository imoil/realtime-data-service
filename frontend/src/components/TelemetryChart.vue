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
import { ref, onMounted, onUnmounted, watch } from 'vue';
import { lightningChart, ChartXY, PointLineAreaSeries, ColorHEX, SolidFill } from '@arction/lcjs';
import { useTelemetryDashboard } from '../composables/useTelemetryDashboard';

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

const SENSOR_PALETTE = [
  '#58a6ff', '#3fb950', '#d29922', '#f85149', '#bc8cff',
  '#f0883e', '#79c0ff', '#56d364', '#e3b341', '#ff7b72'
];

const dashboard = useTelemetryDashboard({
  sensorId: props.sensorId,
  sensorIds: props.sensorIds,
  lbSec: props.lbSec,
  ubSec: props.ubSec,
  width: props.width
});

const {
  currentLbSec,
  currentUbSec,
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
  zoomTo,
  resetToMacro,
  refresh,
  toggleLiveTail,
  loadHistoricalData,
  mountDashboard,
  disposeDashboard,
  onHistoricalData,
  onOverviewData,
  onTick
} = dashboard;

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

// Bind historical data to main chart series
onHistoricalData((decomposed) => {
  if (!chart) return;
  const currentActive = new Set(activeSensors.value);
  for (const [sId, sInstance] of seriesMap.entries()) {
    if (!currentActive.has(sId)) {
      if (typeof sInstance.dispose === 'function') {
        sInstance.dispose();
      }
      seriesMap.delete(sId);
    }
  }

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
});

// Bind 30-day overview outline to ZoomBandChart
onOverviewData((decomposed) => {
  if (!zoomBandChart) return;
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
});

// Live tick listener
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

function handleZoomBandIntervalChange(startMs: number, endMs: number) {
  zoomTo(startMs, endMs);
}

function resetToFullRange() {
  if (zoomBandChart && typeof zoomBandChart.setInterval === 'function') {
    isProgrammaticUpdate.value = true;
    try {
      zoomBandChart.setInterval(props.lbSec * 1000, props.ubSec * 1000);
    } finally {
      isProgrammaticUpdate.value = false;
    }
  }
  resetToMacro();
}

function refreshData() {
  refresh();
}

onMounted(() => {
  if (chartContainer.value) {
    try {
      chart = lightningChart().ChartXY({
        container: chartContainer.value
      });
      chart.setTitle(chartTitle.value);

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
          zoomBandChart.onIntervalChange((start: number, end: number) => {
            zoomTo(start, end);
          });
        }
      }

      mountDashboard();
    } catch (e) {
      console.warn('WebGL / LightningChart initialization note:', e);
    }
  }
});

onUnmounted(() => {
  disposeDashboard();
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
      if (chart) {
        chart.setTitle(chartTitle.value);
      }
      const targetIds = props.sensorIds && props.sensorIds.length > 0 ? props.sensorIds : [props.sensorId ?? 1];
      dashboard.updateSensors(targetIds);
    }
  },
  { deep: true }
);

defineExpose({
  handleZoomBandIntervalChange,
  currentLbSec,
  currentUbSec,
  isProgrammaticUpdate,
  loadData: loadHistoricalData,
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
