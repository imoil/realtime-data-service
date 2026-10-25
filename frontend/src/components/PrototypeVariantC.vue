<template>
  <div class="variant-c">
    <!-- Fullscreen Telemetry Canvas Layer -->
    <div class="cockpit-canvas">
      <div class="cockpit-bar">
        <div class="cockpit-branding">
          <span class="status-indicator live-pulse"></span>
          <strong>COCKPIT LIVE TELEMETRY</strong>
          <span class="table-tag">{{ targetTableInfo }}</span>
        </div>

        <div class="quick-controls">
          <div class="button-group">
            <button
              v-for="preset in SENSOR_PRESETS"
              :key="preset.count"
              class="cockpit-btn"
              :class="{ active: selectedCount === preset.count }"
              @click="applySensorPreset(preset.count)"
            >
              {{ preset.label }}
            </button>
          </div>

          <div class="button-group">
            <button
              v-for="range in RANGE_PRESETS"
              :key="range.seconds"
              class="cockpit-btn"
              :class="{ active: selectedRangeSec === range.seconds }"
              @click="selectedRangeSec = range.seconds"
            >
              {{ range.label }}
            </button>
          </div>
        </div>

        <div class="drawer-triggers">
          <button class="trigger-btn" :class="{ open: activeDrawer === 'formula' }" @click="toggleDrawer('formula')">
            ⚡ Formulas
          </button>
          <button class="trigger-btn" :class="{ open: activeDrawer === 'registry' }" @click="toggleDrawer('registry')">
            📋 Registry
          </button>
          <button class="trigger-btn" :class="{ open: activeDrawer === 'sim' }" @click="toggleDrawer('sim')">
            📈 Sim Studio
          </button>
        </div>
      </div>

      <div class="chart-wrapper">
        <TelemetryChart
          :sensorIds="activeSensorIds"
          :lbSec="lbSec"
          :ubSec="ubSec"
          :width="1920"
        />
      </div>
    </div>

    <!-- Sliding Drawer: Formula Studio -->
    <div v-if="activeDrawer === 'formula'" class="flyout-drawer right-drawer">
      <div class="drawer-header">
        <h3>⚡ Dynamic Formula Engine</h3>
        <button class="close-btn" @click="activeDrawer = null">&times;</button>
      </div>
      <div class="drawer-body">
        <div class="form-group">
          <label>Derived Sensor Name:</label>
          <input v-model="fName" class="drawer-input" />
        </div>
        <div class="form-group">
          <label>AviatorScript Expression:</label>
          <textarea v-model="fExpr" class="drawer-textarea" rows="3"></textarea>
          <span class="field-hint">AST verified via ExpressionValidator</span>
        </div>
        <button class="action-btn broadcast-btn" @click="broadcastRule">Broadcast to Flink</button>
        <div v-if="broadcastSuccess" class="success-alert">✓ Rule broadcast sent to Kafka topic</div>
      </div>
    </div>

    <!-- Sliding Drawer: Sensor Registry -->
    <div v-if="activeDrawer === 'registry'" class="flyout-drawer bottom-drawer">
      <div class="drawer-header">
        <h3>📋 Oracle 19c SENSOR_REGISTRY (Serial Sequence 10001+)</h3>
        <button class="close-btn" @click="activeDrawer = null">&times;</button>
      </div>
      <div class="drawer-body horizontal-scroll">
        <table class="drawer-table">
          <thead>
            <tr>
              <th>Serial</th>
              <th>Code</th>
              <th>Name</th>
              <th>Type</th>
              <th>Device</th>
              <th>Status</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="s in SENSORS" :key="s.serial">
              <td><strong>SN_{{ s.serial }}</strong></td>
              <td><code>{{ s.code }}</code></td>
              <td>{{ s.name }}</td>
              <td><span class="type-pill">{{ s.type }}</span></td>
              <td>{{ s.device }}</td>
              <td><span class="active-dot"></span> ACTIVE</td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>

    <!-- Modal Drawer: Simulation Studio -->
    <div v-if="activeDrawer === 'sim'" class="flyout-drawer right-drawer">
      <div class="drawer-header">
        <h3>📈 On-Demand Formula Sim</h3>
        <button class="close-btn" @click="activeDrawer = null">&times;</button>
      </div>
      <div class="drawer-body">
        <div class="form-group">
          <label>Select Formula:</label>
          <select class="drawer-input">
            <option>Thermal Efficiency (SN_10001 * 0.75 + SN_10002 * 0.25)</option>
            <option>Pressure Peak (window_max(10003, 10m))</option>
          </select>
        </div>
        <div class="form-group">
          <label>ClickHouse Sampling Route:</label>
          <select class="drawer-input">
            <option>1m Rollup (sensor_rollup_1m: 50x Fast)</option>
            <option>1s Raw (sensor_data: Detailed)</option>
          </select>
        </div>
        <button class="action-btn run-btn">Simulate 30 Days</button>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed } from 'vue';
import TelemetryChart from './TelemetryChart.vue';

const activeDrawer = ref<'formula' | 'registry' | 'sim' | null>(null);

function toggleDrawer(name: 'formula' | 'registry' | 'sim') {
  activeDrawer.value = activeDrawer.value === name ? null : name;
}

const fName = ref('Thermal Efficiency');
const fExpr = ref('SN_10001 * 0.75 + SN_10002 * 0.25');
const broadcastSuccess = ref(false);

function broadcastRule() {
  broadcastSuccess.value = true;
  setTimeout(() => { broadcastSuccess.value = false; }, 3000);
}

const SENSORS = [
  { serial: 10001, code: 'PHY-D01-S0', name: 'Bearing Temp Front', type: 'PHYSICAL', device: 'DEVICE-01' },
  { serial: 10002, code: 'PHY-D01-S1', name: 'Bearing Temp Rear', type: 'PHYSICAL', device: 'DEVICE-01' },
  { serial: 10003, code: 'PHY-D01-S2', name: 'Hydraulic Pressure In', type: 'PHYSICAL', device: 'DEVICE-01' },
  { serial: 20001, code: 'FML-PUMP-EFF', name: 'Thermal Efficiency', type: 'FORMULA', device: 'DERIVED' }
];

const SENSOR_PRESETS = [
  { label: '1', count: 1 },
  { label: '10', count: 10 },
  { label: '50', count: 50 }
];

const RANGE_PRESETS = [
  { label: '30D', seconds: 30 * 86400 },
  { label: '7D', seconds: 7 * 86400 },
  { label: '1D', seconds: 86400 },
  { label: '1H', seconds: 3600 },
  { label: '1M', seconds: 60 }
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
    ? 'ClickHouse iot.sensor_rollup_1h'
    : 'ClickHouse iot.sensor_rollup_1m';
});
</script>

<style scoped>
.variant-c {
  position: relative;
  height: calc(100vh - 75px);
  overflow: hidden;
  background: #0d1117;
}

.cockpit-canvas {
  display: flex;
  flex-direction: column;
  height: 100%;
}

.cockpit-bar {
  display: flex;
  justify-content: space-between;
  align-items: center;
  background: rgba(22, 27, 34, 0.9);
  backdrop-filter: blur(8px);
  border-bottom: 1px solid #30363d;
  padding: 8px 16px;
  z-index: 10;
}

.cockpit-branding {
  display: flex;
  align-items: center;
  gap: 8px;
  font-size: 0.85rem;
  color: #f0f6fc;
}

.table-tag {
  background: #21262d;
  color: #58a6ff;
  font-size: 0.75rem;
  padding: 2px 6px;
  border-radius: 4px;
}

.quick-controls {
  display: flex;
  gap: 12px;
}

.button-group {
  display: flex;
  gap: 4px;
}

.cockpit-btn {
  background: #21262d;
  color: #c9d1d9;
  border: 1px solid #30363d;
  padding: 3px 8px;
  border-radius: 4px;
  font-size: 0.75rem;
  cursor: pointer;
}

.cockpit-btn.active {
  background: #1f6feb;
  color: white;
  border-color: #58a6ff;
}

.drawer-triggers {
  display: flex;
  gap: 8px;
}

.trigger-btn {
  background: #21262d;
  border: 1px solid #30363d;
  color: #f0f6fc;
  padding: 5px 12px;
  border-radius: 6px;
  font-size: 0.8rem;
  font-weight: 600;
  cursor: pointer;
  transition: all 0.15s ease;
}

.trigger-btn:hover, .trigger-btn.open {
  background: #1f6feb;
  border-color: #58a6ff;
}

.chart-wrapper {
  flex: 1;
  height: calc(100% - 50px);
}

/* Flyout Drawers */
.flyout-drawer {
  position: absolute;
  background: rgba(22, 27, 34, 0.95);
  backdrop-filter: blur(16px);
  border: 1px solid #30363d;
  box-shadow: 0 16px 48px rgba(0,0,0,0.8);
  z-index: 100;
  display: flex;
  flex-direction: column;
}

.right-drawer {
  top: 48px;
  right: 0;
  width: 400px;
  height: calc(100% - 48px);
  border-right: none;
  border-top: none;
  animation: slideLeft 0.2s ease-out;
}

.bottom-drawer {
  bottom: 0;
  left: 0;
  right: 0;
  height: 280px;
  border-bottom: none;
  animation: slideUp 0.2s ease-out;
}

@keyframes slideLeft {
  from { transform: translateX(100%); }
  to { transform: translateX(0); }
}

@keyframes slideUp {
  from { transform: translateY(100%); }
  to { transform: translateY(0); }
}

.drawer-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 12px 16px;
  border-bottom: 1px solid #30363d;
}

.drawer-header h3 {
  margin: 0;
  font-size: 0.9rem;
  color: #f0f6fc;
}

.close-btn {
  background: transparent;
  border: none;
  color: #8b949e;
  font-size: 1.3rem;
  cursor: pointer;
}

.close-btn:hover {
  color: #f0f6fc;
}

.drawer-body {
  padding: 16px;
  overflow-y: auto;
  display: flex;
  flex-direction: column;
  gap: 12px;
}

.form-group {
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.form-group label {
  font-size: 0.75rem;
  color: #8b949e;
}

.drawer-input, .drawer-textarea {
  background: #0d1117;
  border: 1px solid #30363d;
  color: #f0f6fc;
  padding: 8px 10px;
  border-radius: 4px;
  font-size: 0.85rem;
}

.drawer-textarea {
  font-family: monospace;
  color: #58a6ff;
}

.field-hint {
  font-size: 0.7rem;
  color: #3fb950;
}

.action-btn {
  border: none;
  padding: 9px;
  border-radius: 6px;
  font-weight: 600;
  cursor: pointer;
  font-size: 0.85rem;
}

.broadcast-btn {
  background: #238636;
  color: white;
}

.run-btn {
  background: #1f6feb;
  color: white;
}

.success-alert {
  color: #3fb950;
  font-size: 0.75rem;
  text-align: center;
}

.drawer-table {
  width: 100%;
  border-collapse: collapse;
  font-size: 0.8rem;
}

.drawer-table th, .drawer-table td {
  border: 1px solid #30363d;
  padding: 8px 12px;
  text-align: left;
}

.drawer-table th {
  background: #161b22;
  color: #8b949e;
}

.type-pill {
  font-size: 0.65rem;
  background: rgba(56, 139, 253, 0.2);
  color: #58a6ff;
  padding: 2px 5px;
  border-radius: 3px;
}

.active-dot {
  display: inline-block;
  width: 6px;
  height: 6px;
  border-radius: 50%;
  background: #238636;
  margin-right: 4px;
}

.status-indicator {
  width: 8px;
  height: 8px;
  border-radius: 50%;
  background: #238636;
}

.live-pulse {
  box-shadow: 0 0 8px #238636;
}
</style>
