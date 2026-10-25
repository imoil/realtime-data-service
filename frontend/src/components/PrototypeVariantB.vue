<template>
  <div class="variant-b">
    <!-- Left Docked Studio Panel -->
    <aside class="dock-panel">
      <div class="dock-header">
        <h2>Formula & Sensor Dock</h2>
        <span class="dock-badge">Live Split Mode</span>
      </div>

      <!-- Section 1: Active Formula Quick-Editor -->
      <div class="dock-section">
        <div class="section-header">
          <span>⚡ Dynamic Formula Rule</span>
          <span class="status-indicator-sm"></span>
        </div>
        <div class="field-group">
          <label>Derived Sensor Target:</label>
          <div class="target-row">
            <span class="serial-pill">SN_20001</span>
            <input v-model="formulaName" class="dock-input" />
          </div>
        </div>
        <div class="field-group">
          <label>Expression (AviatorScript):</label>
          <textarea v-model="expression" class="dock-textarea" rows="2"></textarea>
        </div>
        <button class="push-rule-btn" @click="publishRule">
          Broadcast to Flink 1.20
        </button>
        <div v-if="ruleFeedback" class="feedback-msg">{{ ruleFeedback }}</div>
      </div>

      <!-- Section 2: Sensor Serial Mapping Quick Tree -->
      <div class="dock-section">
        <div class="section-header">
          <span>📋 Two-Level Serial Mappings</span>
          <span class="count-tag">4 Active</span>
        </div>
        <div class="serial-tree">
          <div class="device-node">
            <div class="device-label">DEVICE-01 (L1 Cache Route)</div>
            <div class="sensor-leaf" v-for="s in DEVICE_SENSORS" :key="s.serial">
              <span class="sensor-leaf-idx">#{{ s.idx }}</span>
              <span class="sensor-leaf-name">{{ s.name }}</span>
              <span class="serial-highlight">SN_{{ s.serial }}</span>
            </div>
          </div>
        </div>
      </div>

      <!-- Section 3: Flink RocksDB State Monitor -->
      <div class="dock-section">
        <div class="section-header">
          <span>📦 RocksDB State Backend</span>
          <span class="tag-opt">Optimized</span>
        </div>
        <div class="state-metrics">
          <div class="state-metric-row">
            <span>Buffer Structure:</span>
            <strong>CompactCircularDoubleBuffer</strong>
          </div>
          <div class="state-metric-row">
            <span>Sliding Window:</span>
            <strong>30m (1,800 ticks)</strong>
          </div>
          <div class="state-metric-row">
            <span>Heap Allocations:</span>
            <strong class="good-val">0 B (Direct ByteState)</strong>
          </div>
        </div>
      </div>
    </aside>

    <!-- Right Live Canvas Workspace -->
    <section class="main-canvas">
      <div class="canvas-control-bar">
        <div class="bar-group">
          <span class="group-label">Sensors:</span>
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
        </div>

        <div class="bar-group">
          <span class="group-label">Range:</span>
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

        <div class="live-status-pill">
          <span class="status-indicator"></span>
          <span>ClickHouse Direct Arrow: <strong>{{ targetTableInfo }}</strong></span>
        </div>
      </div>

      <div class="canvas-chart">
        <TelemetryChart
          :sensorIds="activeSensorIds"
          :lbSec="lbSec"
          :ubSec="ubSec"
          :width="1540"
        />
      </div>
    </section>
  </div>
</template>

<script setup lang="ts">
import { ref, computed } from 'vue';
import TelemetryChart from './TelemetryChart.vue';

const formulaName = ref('Thermal Efficiency');
const expression = ref('SN_10001 * 0.75 + SN_10002 * 0.25');
const ruleFeedback = ref('');

function publishRule() {
  ruleFeedback.value = '✓ Rule broadcasted to Kafka topic "iot.rule.events"';
  setTimeout(() => { ruleFeedback.value = ''; }, 3500);
}

const DEVICE_SENSORS = [
  { idx: 0, name: 'Bearing Temp Front', serial: 10001 },
  { idx: 1, name: 'Bearing Temp Rear', serial: 10002 },
  { idx: 2, name: 'Hydraulic Pressure In', serial: 10003 },
  { idx: 3, name: 'Hydraulic Pressure Out', serial: 10004 }
];

const SENSOR_PRESETS = [
  { label: 'Single (1)', count: 1 },
  { label: 'Macro (10 Sensors)', count: 10 },
  { label: 'High Density (50 Sensors)', count: 50 }
];

const RANGE_PRESETS = [
  { label: '30 Days', seconds: 30 * 86400 },
  { label: '7 Days', seconds: 7 * 86400 },
  { label: '1 Day', seconds: 86400 },
  { label: '1 Hour', seconds: 3600 },
  { label: '1 Minute', seconds: 60 }
];

const selectedCount = ref(10);
const selectedRangeSec = ref(30 * 86400);
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
    ? 'iot.sensor_rollup_1h'
    : 'iot.sensor_rollup_1m';
});
</script>

<style scoped>
.variant-b {
  display: flex;
  height: calc(100vh - 75px);
  overflow: hidden;
}

.dock-panel {
  width: 380px;
  min-width: 380px;
  background: #161b22;
  border-right: 1px solid #30363d;
  display: flex;
  flex-direction: column;
  overflow-y: auto;
  padding: 16px;
  gap: 16px;
  box-sizing: border-box;
}

.dock-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  border-bottom: 1px solid #30363d;
  padding-bottom: 12px;
}

.dock-header h2 {
  font-size: 1rem;
  font-weight: 600;
  margin: 0;
  color: #f0f6fc;
}

.dock-badge {
  background: rgba(88, 166, 255, 0.15);
  color: #58a6ff;
  border: 1px solid #388bfd;
  font-size: 0.7rem;
  padding: 2px 6px;
  border-radius: 4px;
}

.dock-section {
  background: #0d1117;
  border: 1px solid #30363d;
  border-radius: 6px;
  padding: 12px;
}

.section-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  font-size: 0.85rem;
  font-weight: 600;
  color: #c9d1d9;
  margin-bottom: 10px;
}

.field-group {
  display: flex;
  flex-direction: column;
  gap: 4px;
  margin-bottom: 10px;
}

.field-group label {
  font-size: 0.75rem;
  color: #8b949e;
}

.target-row {
  display: flex;
  gap: 8px;
  align-items: center;
}

.serial-pill {
  background: #1f6feb;
  color: white;
  font-size: 0.75rem;
  font-weight: 600;
  padding: 3px 6px;
  border-radius: 4px;
}

.dock-input, .dock-textarea {
  background: #161b22;
  border: 1px solid #30363d;
  color: #f0f6fc;
  font-size: 0.85rem;
  padding: 6px 10px;
  border-radius: 4px;
  width: 100%;
  box-sizing: border-box;
}

.dock-textarea {
  font-family: monospace;
  resize: vertical;
  color: #58a6ff;
}

.push-rule-btn {
  background: #238636;
  color: white;
  border: none;
  width: 100%;
  padding: 8px;
  border-radius: 4px;
  font-size: 0.85rem;
  font-weight: 600;
  cursor: pointer;
  transition: background 0.15s ease;
}

.push-rule-btn:hover {
  background: #2ea043;
}

.feedback-msg {
  color: #3fb950;
  font-size: 0.75rem;
  margin-top: 6px;
  text-align: center;
}

.count-tag, .tag-opt {
  font-size: 0.7rem;
  background: #21262d;
  color: #8b949e;
  padding: 2px 6px;
  border-radius: 10px;
}

.tag-opt {
  color: #3fb950;
}

.serial-tree {
  font-size: 0.8rem;
}

.device-node {
  border-left: 2px solid #388bfd;
  padding-left: 8px;
}

.device-label {
  color: #58a6ff;
  font-weight: 600;
  margin-bottom: 6px;
}

.sensor-leaf {
  display: flex;
  justify-content: space-between;
  padding: 3px 0;
  border-bottom: 1px solid #21262d;
}

.sensor-leaf-idx {
  color: #8b949e;
  width: 25px;
}

.sensor-leaf-name {
  color: #c9d1d9;
  flex: 1;
}

.serial-highlight {
  color: #bc8cff;
  font-family: monospace;
}

.state-metrics {
  font-size: 0.8rem;
  display: flex;
  flex-direction: column;
  gap: 6px;
}

.state-metric-row {
  display: flex;
  justify-content: space-between;
  color: #8b949e;
}

.state-metric-row strong {
  color: #f0f6fc;
}

.good-val {
  color: #3fb950 !important;
}

/* Right Main Canvas */
.main-canvas {
  flex: 1;
  display: flex;
  flex-direction: column;
  background: #0d1117;
  overflow: hidden;
  padding: 16px;
  gap: 12px;
}

.canvas-control-bar {
  display: flex;
  justify-content: space-between;
  align-items: center;
  background: #161b22;
  border: 1px solid #30363d;
  border-radius: 6px;
  padding: 10px 16px;
  flex-wrap: wrap;
  gap: 10px;
}

.bar-group {
  display: flex;
  align-items: center;
  gap: 10px;
}

.group-label {
  font-size: 0.8rem;
  color: #8b949e;
  font-weight: 600;
}

.button-group {
  display: flex;
  gap: 6px;
}

.preset-btn {
  background-color: #21262d;
  color: #c9d1d9;
  border: 1px solid #30363d;
  padding: 4px 10px;
  border-radius: 4px;
  font-size: 0.8rem;
  cursor: pointer;
}

.preset-btn.active {
  background-color: #1f6feb;
  color: white;
  border-color: #58a6ff;
}

.live-status-pill {
  display: flex;
  align-items: center;
  gap: 8px;
  font-size: 0.8rem;
  color: #8b949e;
}

.status-indicator {
  width: 8px;
  height: 8px;
  border-radius: 50%;
  background: #238636;
  box-shadow: 0 0 6px #238636;
}

.status-indicator-sm {
  width: 6px;
  height: 6px;
  border-radius: 50%;
  background: #238636;
}

.canvas-chart {
  flex: 1;
  height: 100%;
}
</style>
