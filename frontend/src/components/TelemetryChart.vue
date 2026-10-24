<template>
  <div class="telemetry-chart-wrapper">
    <div class="chart-header">
      <div class="title-section">
        <h3>Sensor {{ sensorId }} Telemetry</h3>
        <span class="badge">Walking Skeleton</span>
      </div>
      <div class="stats-section" v-if="currentBatch">
        <span class="stat-item">Points: <strong>{{ currentBatch.length }}</strong></span>
        <span class="stat-item">AM4 Viewport: <strong>{{ width }}px</strong></span>
        <span class="stat-item" v-if="isLoading">Streaming...</span>
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
import { ref, onMounted, onUnmounted, watch } from 'vue';
import { lightningChart, ChartXY, PointLineAreaSeries } from '@arction/lcjs';
import { useTelemetryStream } from '../composables/useTelemetryStream';

const props = withDefaults(
  defineProps<{
    sensorId?: number;
    lbSec?: number;
    ubSec?: number;
    width?: number;
  }>(),
  {
    sensorId: 1,
    lbSec: 1729728000,
    ubSec: 1729728060,
    width: 1920
  }
);

const chartContainer = ref<HTMLDivElement | null>(null);
let chart: ChartXY | null = null;
let series: PointLineAreaSeries | null = null;

const {
  isLoading,
  error,
  isProgrammaticUpdate,
  currentBatch,
  fetchAm4Arrow
} = useTelemetryStream();

async function loadData() {
  try {
    isProgrammaticUpdate.value = true;
    const batch = await fetchAm4Arrow({
      sensorId: props.sensorId,
      lbSec: props.lbSec,
      ubSec: props.ubSec,
      width: props.width
    });

    if (series && batch.length > 0) {
      series.clear();
      // Zero-copy injection of typed arrays into LightningChart WebGL series
      const seriesAny = series as unknown as { appendSamples?: (s: unknown) => void; add: (pts: unknown[]) => void };
      if (typeof seriesAny.appendSamples === 'function') {
        seriesAny.appendSamples({
          xValues: batch.tsMs,
          yValues: batch.val
        });
      } else {
        const points = [];
        for (let i = 0; i < batch.length; i++) {
          points.push({ x: batch.tsMs[i], y: batch.val[i] });
        }
        seriesAny.add(points);
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

      chart.setTitle(`Sensor ${props.sensorId} - Real-time AM4 Rollup Extrema`);

      series = chart.addPointLineAreaSeries();

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
    series = null;
  }
});

watch(
  () => [props.sensorId, props.lbSec, props.ubSec],
  () => {
    if (!isProgrammaticUpdate.value) {
      loadData();
    }
  }
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
  background-color: #238636;
  color: #fff;
  padding: 2px 8px;
  border-radius: 12px;
  font-size: 0.75rem;
  font-weight: 600;
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
  min-height: 450px;
  width: 100%;
  border-radius: 4px;
  background-color: #0d1117;
}
</style>
