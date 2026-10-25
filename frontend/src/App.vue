<template>
  <main class="app-main">
    <header class="app-header">
      <div class="header-inner">
        <div class="brand">
          <h1>5M IoT Telemetry Pipeline & Formula Studio</h1>
          <p class="subtitle">Unified Industrial Stream Processing, Dynamic Rules & High-Density WebGL Telemetry</p>
        </div>
        <div class="header-meta">
          <span class="system-status">
            <span class="status-indicator"></span>
            <span>Flink 1.20 + ClickHouse + Oracle 19c</span>
          </span>
        </div>
      </div>
    </header>

    <!-- Radical UI Variants for Prototype Exploration -->
    <PrototypeVariantA v-if="currentVariant === 'A'" />
    <PrototypeVariantB v-else-if="currentVariant === 'B'" />
    <PrototypeVariantC v-else-if="currentVariant === 'C'" />

    <!-- Floating Prototype Variant Switcher Bar -->
    <PrototypeSwitcher :currentVariant="currentVariant" @update:variant="onVariantChange" />
  </main>
</template>

<script setup lang="ts">
import { ref, onMounted } from 'vue';
import PrototypeSwitcher from './components/PrototypeSwitcher.vue';
import PrototypeVariantA from './components/PrototypeVariantA.vue';
import PrototypeVariantB from './components/PrototypeVariantB.vue';
import PrototypeVariantC from './components/PrototypeVariantC.vue';

const currentVariant = ref<'A' | 'B' | 'C'>('A');

function getVariantFromUrl(): 'A' | 'B' | 'C' {
  const urlParams = new URLSearchParams(window.location.search);
  const v = urlParams.get('variant')?.toUpperCase();
  if (v === 'A' || v === 'B' || v === 'C') {
    return v;
  }
  return 'A';
}

function onVariantChange(newVar: string) {
  if (newVar === 'A' || newVar === 'B' || newVar === 'C') {
    currentVariant.value = newVar;
  }
}

onMounted(() => {
  currentVariant.value = getVariantFromUrl();
  window.addEventListener('popstate', () => {
    currentVariant.value = getVariantFromUrl();
  });
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
  padding: 14px 24px;
}

.header-inner {
  display: flex;
  justify-content: space-between;
  align-items: center;
  max-width: 1400px;
  margin: 0 auto;
}

.brand h1 {
  margin: 0;
  font-size: 1.25rem;
  font-weight: 600;
  color: #f0f6fc;
}

.subtitle {
  margin: 4px 0 0;
  font-size: 0.8rem;
  color: #8b949e;
}

.header-meta {
  display: flex;
  align-items: center;
  gap: 12px;
}

.system-status {
  display: flex;
  align-items: center;
  gap: 8px;
  font-size: 0.8rem;
  color: #8b949e;
  background-color: #0d1117;
  padding: 5px 12px;
  border-radius: 6px;
  border: 1px solid #30363d;
}

.status-indicator {
  width: 8px;
  height: 8px;
  border-radius: 50%;
  background-color: #238636;
  box-shadow: 0 0 8px #238636;
}
</style>
