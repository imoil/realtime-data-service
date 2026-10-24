<template>
  <main class="app-main">
    <header class="app-header">
      <div class="header-inner">
        <div>
          <h1>5M IoT Telemetry Pipeline</h1>
          <p class="subtitle">High-Density Multi-Sensor 30-Day AM4 Telemetry View</p>
        </div>
        <div class="system-status">
          <span class="status-indicator"></span>
          <span>Target: <strong>{{ targetTableInfo }}</strong></span>
        </div>
      </div>
    </header>

    <div class="content-container">
      <div class="control-panel">
        <div class="panel-group">
          <span class="group-label">Sensor Selection:</span>
          <div class="button-group">
            <button
              v-for="preset in SENSOR_PRESETS"
              :key="preset.count"
              class="preset-btn"
              :class="{ active: selectedCount === preset.count }"
              @click="applySensorPreset(preset.count)"
            >
              {{ preset.label }}
            </button>
          </div>
          <span class="sensor-count-badge">{{ activeSensorIds.length }} Sensors Active</span>
        </div>

        <div class="panel-group">
          <span class="group-label">Macro / Micro Time Range:</span>
          <div class="button-group">
            <button
              v-for="range in RANGE_PRESETS"
              :key="range.seconds"
              class="preset-btn"
              :class="{ active: selectedRangeSec === range.seconds }"
              @click="selectedRangeSec = range.seconds"
            >
              {{ range.label }}
            </button>
          </div>
        </div>
      </div>

      <div class="chart-section">
        <TelemetryChart
          :sensorIds="activeSensorIds"
          :lbSec="lbSec"
          :ubSec="ubSec"
          :width="1920"
        />
      </div>
    </div>
  </main>
</template>

<script setup lang="ts">
import { ref, computed } from 'vue';
import TelemetryChart from './components/TelemetryChart.vue';

const SENSOR_PRESETS = [
  { label: 'Single (1)', count: 1 },
  { label: 'Macro (10 Sensors)', count: 10 },
  { label: 'High Density (50 Sensors)', count: 50 }
];

const RANGE_PRESETS = [
  { label: '30 Days (Macro AM4)', seconds: 30 * 86400 },
  { label: '7 Days', seconds: 7 * 86400 },
  { label: '1 Day', seconds: 86400 },
  { label: '1 Hour', seconds: 3600 },
  { label: '1 Minute', seconds: 60 }
];

const selectedCount = ref(10);
const selectedRangeSec = ref(30 * 86400); // 30-day default

const baseEpoch = 1729728000;
const lbSec = computed(() => baseEpoch);
const ubSec = computed(() => baseEpoch + selectedRangeSec.value);

const activeSensorIds = ref<number[]>(Array.from({ length: 10 }, (_, i) => i + 1));

function applySensorPreset(count: number) {
  selectedCount.value = count;
  activeSensorIds.value = Array.from({ length: count }, (_, i) => i + 1);
}

const targetTableInfo = computed(() => {
  return selectedRangeSec.value > 7 * 86400
    ? 'ClickHouse iot.sensor_rollup_1h'
    : 'ClickHouse iot.sensor_rollup_1m';
});
</script>

<style>
body {
  margin: 0;
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;
  background-color: #0d1117;
  color: #c9d1d9;
}

.app-main {
  display: flex;
  flex-direction: column;
  min-height: 100vh;
}

.app-header {
  border-bottom: 1px solid #30363d;
  background-color: #161b22;
  padding: 16px 24px;
}

.header-inner {
  display: flex;
  justify-content: space-between;
  align-items: center;
  max-width: 1400px;
  margin: 0 auto;
}

.header-inner h1 {
  margin: 0;
  font-size: 1.35rem;
  font-weight: 600;
  color: #f0f6fc;
}

.subtitle {
  margin: 4px 0 0;
  font-size: 0.85rem;
  color: #8b949e;
}

.system-status {
  display: flex;
  align-items: center;
  gap: 8px;
  font-size: 0.85rem;
  color: #8b949e;
  background-color: #0d1117;
  padding: 6px 12px;
  border-radius: 6px;
  border: 1px solid #30363d;
}

.system-status strong {
  color: #58a6ff;
}

.status-indicator {
  width: 8px;
  height: 8px;
  border-radius: 50%;
  background-color: #238636;
  box-shadow: 0 0 8px #238636;
}

.content-container {
  max-width: 1400px;
  width: 100%;
  margin: 0 auto;
  padding: 24px;
  box-sizing: border-box;
}

.control-panel {
  display: flex;
  flex-direction: column;
  gap: 14px;
  margin-bottom: 20px;
  background-color: #161b22;
  padding: 16px 20px;
  border-radius: 6px;
  border: 1px solid #30363d;
}

.panel-group {
  display: flex;
  align-items: center;
  gap: 16px;
  flex-wrap: wrap;
}

.group-label {
  font-size: 0.85rem;
  font-weight: 600;
  color: #8b949e;
  min-width: 140px;
}

.button-group {
  display: flex;
  gap: 8px;
  flex-wrap: wrap;
}

.preset-btn {
  background-color: #21262d;
  color: #c9d1d9;
  border: 1px solid #30363d;
  padding: 6px 14px;
  border-radius: 6px;
  cursor: pointer;
  font-size: 0.85rem;
  transition: all 0.15s ease;
}

.preset-btn:hover {
  background-color: #30363d;
  border-color: #8b949e;
}

.preset-btn.active {
  background-color: #1f6feb;
  color: #ffffff;
  border-color: #58a6ff;
  font-weight: 600;
}

.sensor-count-badge {
  background-color: #238636;
  color: #ffffff;
  font-size: 0.75rem;
  font-weight: 600;
  padding: 3px 8px;
  border-radius: 12px;
}

.chart-section {
  height: 580px;
}
</style>
