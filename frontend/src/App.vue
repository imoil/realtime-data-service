<template>
  <main class="app-main">
    <header class="app-header">
      <div class="header-inner">
        <h1>5M IoT Telemetry Pipeline</h1>
        <div class="system-status">
          <span class="status-indicator"></span>
          <span>Single-Sensor Walking Skeleton</span>
        </div>
      </div>
    </header>

    <div class="content-container">
      <div class="control-panel">
        <label>
          Sensor ID:
          <input type="number" v-model.number="selectedSensorId" min="1" max="5000000" />
        </label>
        <label>
          Window (Secs):
          <select v-model.number="windowDuration">
            <option :value="60">1 Minute (60s)</option>
            <option :value="300">5 Minutes (300s)</option>
            <option :value="3600">1 Hour (3600s)</option>
          </select>
        </label>
      </div>

      <div class="chart-section">
        <TelemetryChart
          :sensorId="selectedSensorId"
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

const selectedSensorId = ref(1);
const windowDuration = ref(60);

const baseEpoch = 1729728000;
const lbSec = computed(() => baseEpoch);
const ubSec = computed(() => baseEpoch + windowDuration.value);
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

.system-status {
  display: flex;
  align-items: center;
  gap: 8px;
  font-size: 0.9rem;
  color: #8b949e;
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
  gap: 24px;
  margin-bottom: 20px;
  background-color: #161b22;
  padding: 14px 20px;
  border-radius: 6px;
  border: 1px solid #30363d;
}

.control-panel label {
  display: flex;
  align-items: center;
  gap: 8px;
  font-size: 0.9rem;
  color: #8b949e;
}

.control-panel input,
.control-panel select {
  background-color: #0d1117;
  border: 1px solid #30363d;
  color: #c9d1d9;
  padding: 6px 10px;
  border-radius: 4px;
}

.chart-section {
  height: 550px;
}
</style>
