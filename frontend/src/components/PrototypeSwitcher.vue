<template>
  <div class="prototype-switcher" role="toolbar" aria-label="Prototype Variant Switcher">
    <button
      class="nav-arrow"
      @click="prevVariant"
      title="Previous Variant (Left Arrow key)"
      aria-label="Previous Variant"
    >
      &larr;
    </button>
    <div class="variant-info">
      <span class="badge">PROTOTYPE</span>
      <span class="variant-label">
        <strong>Variant {{ currentVariant }}:</strong> {{ currentVariantName }}
      </span>
    </div>
    <button
      class="nav-arrow"
      @click="nextVariant"
      title="Next Variant (Right Arrow key)"
      aria-label="Next Variant"
    >
      &rarr;
    </button>
  </div>
</template>

<script setup lang="ts">
import { computed, onMounted, onUnmounted } from 'vue';

const props = defineProps<{
  currentVariant: string;
}>();

const emit = defineEmits<{
  (e: 'update:variant', val: string): void;
}>();

const VARIANTS = [
  { key: 'A', name: 'Tabbed Industrial Workspace' },
  { key: 'B', name: 'Split-Screen Docked Canvas' },
  { key: 'C', name: 'Operational Cockpit & Flyout Drawers' }
];

const currentVariantName = computed(() => {
  const v = VARIANTS.find(x => x.key === props.currentVariant);
  return v ? v.name : 'Unknown Variant';
});

function setVariant(key: string) {
  const url = new URL(window.location.href);
  url.searchParams.set('variant', key);
  window.history.replaceState({}, '', url.toString());
  emit('update:variant', key);
}

function prevVariant() {
  const idx = VARIANTS.findIndex(x => x.key === props.currentVariant);
  const prevIdx = (idx - 1 + VARIANTS.length) % VARIANTS.length;
  setVariant(VARIANTS[prevIdx].key);
}

function nextVariant() {
  const idx = VARIANTS.findIndex(x => x.key === props.currentVariant);
  const nextIdx = (idx + 1) % VARIANTS.length;
  setVariant(VARIANTS[nextIdx].key);
}

function handleKeydown(e: KeyboardEvent) {
  const target = e.target as HTMLElement | null;
  if (target && (target.tagName === 'INPUT' || target.tagName === 'TEXTAREA' || target.isContentEditable)) {
    return;
  }
  if (e.key === 'ArrowLeft') {
    prevVariant();
  } else if (e.key === 'ArrowRight') {
    nextVariant();
  }
}

onMounted(() => {
  window.addEventListener('keydown', handleKeydown);
});

onUnmounted(() => {
  window.removeEventListener('keydown', handleKeydown);
});
</script>

<style scoped>
.prototype-switcher {
  position: fixed;
  bottom: 24px;
  left: 50%;
  transform: translateX(-50%);
  display: flex;
  align-items: center;
  gap: 12px;
  background: rgba(22, 27, 34, 0.95);
  backdrop-filter: blur(12px);
  border: 1px solid #58a6ff;
  border-radius: 9999px;
  padding: 8px 16px;
  box-shadow: 0 8px 24px rgba(0, 0, 0, 0.6), 0 0 12px rgba(88, 166, 255, 0.3);
  z-index: 99999;
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
  color: #f0f6fc;
}

.nav-arrow {
  background: #21262d;
  color: #58a6ff;
  border: 1px solid #30363d;
  border-radius: 50%;
  width: 28px;
  height: 28px;
  display: flex;
  align-items: center;
  justify-content: center;
  cursor: pointer;
  font-size: 16px;
  font-weight: bold;
  transition: all 0.15s ease;
}

.nav-arrow:hover {
  background: #388bfd;
  color: #ffffff;
  border-color: #58a6ff;
  transform: scale(1.1);
}

.variant-info {
  display: flex;
  align-items: center;
  gap: 10px;
}

.badge {
  background: #bc8cff;
  color: #120c1f;
  font-size: 0.7rem;
  font-weight: 800;
  padding: 2px 7px;
  border-radius: 4px;
  letter-spacing: 0.5px;
}

.variant-label {
  font-size: 0.85rem;
  color: #c9d1d9;
}

.variant-label strong {
  color: #ffffff;
}
</style>
