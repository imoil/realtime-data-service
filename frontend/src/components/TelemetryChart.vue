<template>
  <div class="telemetry-chart-wrapper">
    <div class="chart-header">
      <div class="title-section">
        <h3>{{ chartTitle }}</h3>
        <span class="badge" :class="isMacroRange ? 'badge-macro' : 'badge-intermediate'">
          {{ isMacroRange ? '30-Day Macro (1h Rollup)' : 'Intermediate (1m Rollup)' }}
        </span>
      </div>
      <div class="stats-section" v-if="currentBatch">
        <span class="stat-item">Active Sensors: <strong>{{ activeSensorsCount }}</strong></span>
        <span class="stat-item">Total Extrema Points: <strong>{{ currentBatch.length }}</strong></span>
        <span class="stat-item">Viewport: <strong>{{ width }}px</strong></span>
        <span class="stat-item loading-text" v-if="isLoading">Streaming...</span>
      </div>
      <div class="actions-section">
        <button class="btn btn-primary" :disabled="isLoading" @click="loadData">
          {{ isLoading ? 'Loading...' : 'Refresh' }}
        </button>
      </div>
    </div>

    <div v-if="error" class="error-banner">
      Streaming Error: {{ error }}
    </div>

    <div ref="chartContainer" class="chart-container"></div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted, onUnmounted, watch } from 'vue';
import { lightningChart, ChartXY, PointLineAreaSeries, ColorHEX, SolidFill } from '@arction/lcjs';
import { useTelemetryStream } from '../composables/useTelemetryStream';

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
let chart: ChartXY | null = null;
const seriesMap = new Map<number, PointLineAreaSeries>();

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
  return (props.ubSec - props.lbSec) > (7 * 86400);
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
  currentBatch,
  decomposeBySensor,
  fetchAm4Arrow
} = useTelemetryStream();

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

  // Assign distinct theme palette color
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

async function loadData() {
  try {
    isProgrammaticUpdate.value = true;
    const batch = await fetchAm4Arrow({
      sensorIds: targetSensorIds.value,
      lbSec: props.lbSec,
      ubSec: props.ubSec,
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

      // Inject zero-copy slices into each sensor's series
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
  } catch (err) {
    console.error('Failed to stream telemetry:', err);
  } finally {
    isProgrammaticUpdate.value = false;
  }
}

onMounted(() => {
  if (chartContainer.value) {
    try {
      chart = lightningChart().ChartXY({
        container: chartContainer.value
      });

      chart.setTitle(chartTitle.value);
      loadData();
    } catch (e) {
      console.warn('WebGL / LightningChart initialization note:', e);
    }
  }
});

onUnmounted(() => {
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
      loadData();
    }
  },
  { deep: true }
);
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

.stats-section {
  display: flex;
  gap: 16px;
  font-size: 0.85rem;
  color: #8b949e;
}

.stat-item strong {
  color: #58a6ff;
}

.loading-text {
  color: #d29922;
  font-weight: 600;
}

.btn {
  background-color: #238636;
  color: #fff;
  border: 1px solid rgba(240, 246, 252, 0.1);
  padding: 6px 14px;
  border-radius: 6px;
  cursor: pointer;
  font-weight: 500;
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
  min-height: 480px;
  width: 100%;
  border-radius: 4px;
  background-color: #0d1117;
}
</style>
