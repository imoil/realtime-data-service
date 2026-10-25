<template>
  <div class="variant-a">
    <!-- Top Nav Tabs -->
    <nav class="sub-nav">
      <div class="sub-nav-inner">
        <button
          v-for="tab in TABS"
          :key="tab.id"
          class="tab-btn"
          :class="{ active: currentTab === tab.id }"
          @click="currentTab = tab.id"
        >
          <span class="tab-icon">{{ tab.icon }}</span>
          <span class="tab-label">{{ tab.label }}</span>
          <span v-if="tab.badge" class="tab-badge">{{ tab.badge }}</span>
        </button>
      </div>
    </nav>

    <!-- Tab 1: Real-Time Telemetry -->
    <div v-if="currentTab === 'telemetry'" class="tab-content">
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

    <!-- Tab 2: Formula Studio & DAG with Right Split Preview -->
    <div v-else-if="currentTab === 'formulas'" class="tab-content formula-studio">
      <div class="studio-split-grid">
        <!-- Column 1: Formula Library -->
        <div class="studio-sidebar">
          <div class="section-title">Defined Dynamic Formulas</div>
          <div class="formula-cards">
            <div
              v-for="f in SAMPLE_FORMULAS"
              :key="f.id"
              class="formula-card"
              :class="{ selected: selectedFormula.id === f.id }"
              @click="selectedFormula = f"
            >
              <div class="formula-header">
                <strong>{{ f.name }}</strong>
                <span class="serial-pill">SN_{{ f.serialNumber }}</span>
              </div>
              <div class="formula-code">{{ f.expression }}</div>
            </div>
          </div>
        </div>

        <!-- Column 2: Formula Editor & DAG -->
        <div class="studio-main">
          <div class="editor-header">
            <h3>Formula Editor & AviatorScript Sandbox</h3>
            <span class="status-badge valid">Security Sandbox Active</span>
          </div>

          <div class="editor-box">
            <label class="input-label">Formula Expression (AviatorScript):</label>
            <input v-model="selectedFormula.expression" class="formula-input" />
            <p class="hint">Refer to physical sensors via <code>SN_&lt;serialNumber&gt;</code> (e.g. <code>SN_10001 * 0.8 + SN_10002 * 0.2</code>) or sliding window functions like <code>window_avg(10001, 30m)</code>.</p>
          </div>

          <div class="dag-preview">
            <h4>DAG Dependency Tree Preview</h4>
            <div class="dag-flow">
              <div class="dag-node input-node">
                <span class="node-type">Physical Input</span>
                <span class="node-id">SN_10001 (Temp #1)</span>
              </div>
              <span class="dag-edge">&rarr;</span>
              <div class="dag-node input-node">
                <span class="node-type">Physical Input</span>
                <span class="node-id">SN_10002 (Temp #2)</span>
              </div>
              <span class="dag-edge">&rarr;</span>
              <div class="dag-node op-node">
                <span class="node-type">Flink 1.20 JIT</span>
                <span class="node-id">{{ selectedFormula.name }}</span>
              </div>
              <span class="dag-edge">&rarr;</span>
              <div class="dag-node out-node">
                <span class="node-type">Derived Sink</span>
                <span class="node-id">ClickHouse (SN_{{ selectedFormula.serialNumber }})</span>
              </div>
            </div>
          </div>
        </div>

        <!-- Column 3: Right-Hand Live Split Waveform Preview -->
        <div class="studio-preview-pane">
          <div class="preview-header">
            <h4>📊 Right Split Preview</h4>
            <span class="preview-badge">Live Sub-Second</span>
          </div>
          <div class="preview-chart-box">
            <div class="mock-waveform-container">
              <svg class="waveform-svg" viewBox="0 0 300 120">
                <path d="M 0,60 Q 30,10 60,60 T 120,60 T 180,60 T 240,60 T 300,60" fill="none" stroke="#388bfd" stroke-width="2" />
                <path d="M 0,70 Q 30,30 60,70 T 120,70 T 180,70 T 240,70 T 300,70" fill="none" stroke="#bc8cff" stroke-width="2" stroke-dasharray="4" />
              </svg>
              <div class="waveform-legend">
                <span class="legend-item"><span class="dot blue"></span> SN_10001 (Raw)</span>
                <span class="legend-item"><span class="dot purple"></span> SN_{{ selectedFormula.serialNumber }} (Derived)</span>
              </div>
            </div>
            <div class="preview-stats">
              <div class="stat-box">
                <span class="stat-lbl">Output Rate</span>
                <span class="stat-val">1 tick/sec</span>
              </div>
              <div class="stat-box">
                <span class="stat-lbl">Flink JIT Latency</span>
                <span class="stat-val">&lt; 0.15 ms</span>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>

    <!-- Tab 3: On-Demand Simulation -->
    <div v-else-if="currentTab === 'simulation'" class="tab-content simulation-view">
      <div class="sim-panel">
        <div class="section-title">On-Demand Time-Series Simulation Studio</div>
        <p class="subtitle">Simulate formulas against ClickHouse historical rollups with sub-second response.</p>
        <div class="sim-controls">
          <div class="ctrl-col">
            <label>Target Formula:</label>
            <select v-model="simFormulaId" class="sim-select">
              <option v-for="f in SAMPLE_FORMULAS" :key="f.id" :value="f.id">{{ f.name }} ({{ f.expression }})</option>
            </select>
          </div>
          <div class="ctrl-col">
            <label>Sampling Interval (ClickHouse Route):</label>
            <select v-model="simSampleBy" class="sim-select">
              <option value="1s">1 Second (Raw sensor_data scan)</option>
              <option value="1m">1 Minute (sensor_rollup_1m: 50x Fast)</option>
              <option value="1h">1 Hour (sensor_rollup_1h: Macro View)</option>
            </select>
          </div>
          <div class="ctrl-col">
            <label>&nbsp;</label>
            <button class="run-sim-btn" @click="runSimulation">⚡ Execute On-Demand Run</button>
          </div>
        </div>

        <div v-if="simResult" class="sim-metrics">
          <div class="metric-pill">Target Table: <strong>{{ simResult.table }}</strong></div>
          <div class="metric-pill">Data Points Evaluated: <strong>{{ simResult.points.toLocaleString() }}</strong></div>
          <div class="metric-pill">Execution Latency: <strong>{{ simResult.latencyMs }} ms (Java 25 VT)</strong></div>
        </div>
      </div>
    </div>

    <!-- Tab 4: Sensor Registry -->
    <div v-else-if="currentTab === 'registry'" class="tab-content registry-view">
      <div class="registry-header">
        <div>
          <h3>Oracle 19c Unified Sensor Registry</h3>
          <p class="subtitle">Global SerialNumber Sequence (10001+) mapped to IoT Devices</p>
        </div>
        <button class="add-btn">+ Register Sensor</button>
      </div>

      <table class="registry-table">
        <thead>
          <tr>
            <th>Serial #</th>
            <th>Sensor Code</th>
            <th>Name</th>
            <th>Type</th>
            <th>Device ID</th>
            <th>Local Index</th>
            <th>Unit</th>
            <th>Status</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="s in SAMPLE_SENSORS" :key="s.serialNumber">
            <td><strong class="serial-highlight">{{ s.serialNumber }}</strong></td>
            <td><code>{{ s.sensorCode }}</code></td>
            <td>{{ s.name }}</td>
            <td><span class="type-tag" :class="s.type.toLowerCase()">{{ s.type }}</span></td>
            <td>{{ s.deviceId || '-' }}</td>
            <td>{{ s.localIndex !== null ? s.localIndex : '-' }}</td>
            <td>{{ s.unit }}</td>
            <td><span class="status-indicator-sm"></span> ACTIVE</td>
          </tr>
        </tbody>
      </table>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed } from 'vue';
import TelemetryChart from './TelemetryChart.vue';

const TABS = [
  { id: 'telemetry', label: 'Real-Time Telemetry', icon: '📊', badge: '5M Live' },
  { id: 'formulas', label: 'Formula Studio & DAG', icon: '⚡', badge: 'Aviator JIT' },
  { id: 'simulation', label: 'On-Demand Simulation', icon: '📈' },
  { id: 'registry', label: 'Sensor Registry', icon: '📋', badge: 'Oracle 19c' }
];

const currentTab = ref('telemetry');

// Telemetry Controls
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
const selectedRangeSec = ref(30 * 86400);
const baseEpoch = 1729728000;
const lbSec = computed(() => baseEpoch);
const ubSec = computed(() => baseEpoch + selectedRangeSec.value);
const activeSensorIds = ref<number[]>(Array.from({ length: 10 }, (_, i) => i + 1));

function applySensorPreset(count: number) {
  selectedCount.value = count;
  activeSensorIds.value = Array.from({ length: count }, (_, i) => i + 1);
}

// Sample Data
const SAMPLE_FORMULAS = [
  { id: 1, serialNumber: 20001, name: 'Pump Thermal Efficiency', expression: 'SN_10001 * 0.75 + SN_10002 * 0.25' },
  { id: 2, serialNumber: 20002, name: '30m Moving Avg Pressure', expression: 'window_avg(10003, 30m) * 1.05' },
  { id: 3, serialNumber: 20003, name: 'Turbine Vibration Index', expression: 'math.sqrt(SN_10004 * SN_10004 + SN_10005 * SN_10005)' }
];
const selectedFormula = ref(SAMPLE_FORMULAS[0]);

const simFormulaId = ref(1);
const simSampleBy = ref('1m');
const simResult = ref<{ table: string; points: number; latencyMs: number } | null>(null);

function runSimulation() {
  const table = simSampleBy.value === '1s' ? 'iot.sensor_data (raw)' : 'iot.sensor_rollup_1m (AM4 pre-aggregated)';
  simResult.value = {
    table,
    points: simSampleBy.value === '1s' ? 86400 : 1440,
    latencyMs: simSampleBy.value === '1s' ? 42 : 8
  };
}

const SAMPLE_SENSORS = [
  { serialNumber: 10001, sensorCode: 'PHY-DEV01-TEMP-0', name: 'Bearing Temp Front', type: 'PHYSICAL', deviceId: 'DEVICE-01', localIndex: 0, unit: '°C' },
  { serialNumber: 10002, sensorCode: 'PHY-DEV01-TEMP-1', name: 'Bearing Temp Rear', type: 'PHYSICAL', deviceId: 'DEVICE-01', localIndex: 1, unit: '°C' },
  { serialNumber: 10003, sensorCode: 'PHY-DEV01-PRES-0', name: 'Hydraulic Pressure In', type: 'PHYSICAL', deviceId: 'DEVICE-01', localIndex: 2, unit: 'bar' },
  { serialNumber: 20001, sensorCode: 'FML-PUMP-EFF-01', name: 'Pump Thermal Efficiency', type: 'FORMULA', deviceId: null, localIndex: null, unit: '%' }
];
</script>

<style scoped>
.variant-a {
  display: flex;
  flex-direction: column;
}

.sub-nav {
  background: #161b22;
  border-bottom: 1px solid #30363d;
  padding: 0 24px;
}

.sub-nav-inner {
  max-width: 1400px;
  margin: 0 auto;
  display: flex;
  gap: 8px;
}

.tab-btn {
  background: transparent;
  border: none;
  border-bottom: 2px solid transparent;
  color: #8b949e;
  padding: 12px 18px;
  cursor: pointer;
  display: flex;
  align-items: center;
  gap: 8px;
  font-size: 0.9rem;
  font-weight: 500;
  transition: all 0.15s ease;
}

.tab-btn:hover {
  color: #f0f6fc;
}

.tab-btn.active {
  color: #58a6ff;
  border-bottom-color: #58a6ff;
  font-weight: 600;
}

.tab-badge {
  background: #21262d;
  color: #58a6ff;
  font-size: 0.7rem;
  padding: 2px 6px;
  border-radius: 10px;
  border: 1px solid #30363d;
}

.tab-content {
  max-width: 1400px;
  width: 100%;
  margin: 0 auto;
  padding: 24px;
  box-sizing: border-box;
}

/* Control Panel */
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

/* Formula Studio Split Grid */
.studio-split-grid {
  display: grid;
  grid-template-columns: 280px 1fr 340px;
  gap: 18px;
}

.studio-sidebar, .studio-main, .studio-preview-pane, .sim-panel {
  background: #161b22;
  border: 1px solid #30363d;
  border-radius: 8px;
  padding: 18px;
}

.section-title {
  font-size: 1rem;
  font-weight: 600;
  color: #f0f6fc;
  margin-bottom: 12px;
}

.formula-cards {
  display: flex;
  flex-direction: column;
  gap: 10px;
}

.formula-card {
  background: #0d1117;
  border: 1px solid #30363d;
  border-radius: 6px;
  padding: 10px;
  cursor: pointer;
  transition: all 0.15s ease;
}

.formula-card:hover {
  border-color: #58a6ff;
}

.formula-card.selected {
  border-color: #58a6ff;
  background: #1c2128;
}

.formula-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 6px;
}

.serial-pill {
  background: #1f6feb;
  color: #ffffff;
  font-size: 0.7rem;
  padding: 2px 6px;
  border-radius: 4px;
}

.formula-code {
  font-family: monospace;
  font-size: 0.75rem;
  color: #58a6ff;
}

.editor-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 14px;
}

.editor-header h3 {
  margin: 0;
  font-size: 1rem;
  color: #f0f6fc;
}

.status-badge.valid {
  background: rgba(35, 134, 54, 0.2);
  color: #3fb950;
  border: 1px solid #238636;
  font-size: 0.75rem;
  padding: 3px 8px;
  border-radius: 6px;
}

.formula-input {
  width: 100%;
  background: #0d1117;
  border: 1px solid #30363d;
  color: #58a6ff;
  font-family: monospace;
  font-size: 0.95rem;
  padding: 10px;
  border-radius: 6px;
  margin-top: 6px;
  box-sizing: border-box;
}

.hint {
  font-size: 0.75rem;
  color: #8b949e;
  margin-top: 6px;
}

.dag-preview {
  margin-top: 18px;
  padding-top: 16px;
  border-top: 1px solid #30363d;
}

.dag-preview h4 {
  margin: 0 0 10px;
  font-size: 0.85rem;
  color: #c9d1d9;
}

.dag-flow {
  display: flex;
  align-items: center;
  gap: 10px;
  flex-wrap: wrap;
}

.dag-node {
  background: #0d1117;
  border: 1px solid #30363d;
  border-radius: 6px;
  padding: 6px 12px;
  display: flex;
  flex-direction: column;
}

.dag-node.input-node {
  border-color: #388bfd;
}

.dag-node.op-node {
  border-color: #bc8cff;
}

.dag-node.out-node {
  border-color: #238636;
}

.node-type {
  font-size: 0.65rem;
  color: #8b949e;
  text-transform: uppercase;
}

.node-id {
  font-size: 0.8rem;
  font-weight: 600;
  color: #f0f6fc;
}

.dag-edge {
  color: #8b949e;
  font-size: 1.1rem;
}

/* Right Split Preview Pane */
.studio-preview-pane {
  display: flex;
  flex-direction: column;
  gap: 12px;
}

.preview-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.preview-header h4 {
  margin: 0;
  font-size: 0.9rem;
  color: #f0f6fc;
}

.preview-badge {
  background: rgba(56, 139, 253, 0.2);
  color: #58a6ff;
  font-size: 0.7rem;
  padding: 2px 6px;
  border-radius: 4px;
}

.preview-chart-box {
  background: #0d1117;
  border: 1px solid #30363d;
  border-radius: 6px;
  padding: 12px;
  display: flex;
  flex-direction: column;
  gap: 12px;
}

.waveform-svg {
  width: 100%;
  height: 100px;
  background: #161b22;
  border-radius: 4px;
}

.waveform-legend {
  display: flex;
  justify-content: space-around;
  font-size: 0.75rem;
  color: #8b949e;
}

.legend-item {
  display: flex;
  align-items: center;
  gap: 6px;
}

.dot {
  width: 8px;
  height: 8px;
  border-radius: 50%;
}

.dot.blue {
  background: #388bfd;
}

.dot.purple {
  background: #bc8cff;
}

.preview-stats {
  display: flex;
  justify-content: space-between;
  border-top: 1px solid #21262d;
  padding-top: 8px;
}

.stat-box {
  display: flex;
  flex-direction: column;
}

.stat-lbl {
  font-size: 0.7rem;
  color: #8b949e;
}

.stat-val {
  font-size: 0.8rem;
  font-weight: 600;
  color: #3fb950;
}

/* Simulation View */
.sim-controls {
  display: flex;
  gap: 16px;
  align-items: flex-end;
  margin-top: 16px;
}

.ctrl-col {
  display: flex;
  flex-direction: column;
  gap: 6px;
}

.sim-select {
  background: #0d1117;
  border: 1px solid #30363d;
  color: #c9d1d9;
  padding: 8px 12px;
  border-radius: 6px;
}

.run-sim-btn {
  background: #1f6feb;
  color: white;
  border: none;
  padding: 9px 18px;
  border-radius: 6px;
  font-weight: 600;
  cursor: pointer;
}

.sim-metrics {
  display: flex;
  gap: 16px;
  margin-top: 20px;
}

.metric-pill {
  background: #0d1117;
  border: 1px solid #30363d;
  padding: 8px 14px;
  border-radius: 6px;
  font-size: 0.85rem;
}

/* Registry View */
.registry-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 12px;
}

.registry-header h3 {
  margin: 0;
  color: #f0f6fc;
}

.registry-table {
  width: 100%;
  border-collapse: collapse;
  margin-top: 12px;
  font-size: 0.85rem;
}

.registry-table th, .registry-table td {
  border: 1px solid #30363d;
  padding: 10px 14px;
  text-align: left;
}

.registry-table th {
  background: #161b22;
  color: #8b949e;
}

.registry-table tr:nth-child(even) {
  background: #161b22;
}

.serial-highlight {
  color: #58a6ff;
}

.type-tag {
  font-size: 0.7rem;
  padding: 2px 6px;
  border-radius: 4px;
  font-weight: 600;
}

.type-tag.physical {
  background: rgba(56, 139, 253, 0.2);
  color: #58a6ff;
}

.type-tag.formula {
  background: rgba(188, 140, 255, 0.2);
  color: #bc8cff;
}

.status-indicator-sm {
  display: inline-block;
  width: 6px;
  height: 6px;
  background: #238636;
  border-radius: 50%;
  margin-right: 4px;
}

.add-btn {
  background: #238636;
  color: white;
  border: none;
  padding: 6px 14px;
  border-radius: 6px;
  cursor: pointer;
  font-weight: 600;
}
</style>
