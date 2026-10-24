import { ref, shallowRef } from 'vue';

export interface TelemetryTick {
  sensorId: number;
  timestampMs: number;
  value: number;
}

export function useLiveTelemetryTail() {
  const isConnected = ref(false);
  const tickCount = ref(0);
  const lastTick = shallowRef<TelemetryTick | null>(null);
  const isLiveEnabled = ref(true);
  const activeSubscribedSensors = ref<number[]>([]);

  let socket: WebSocket | null = null;
  const tickListeners: Array<(tick: TelemetryTick) => void> = [];

  function onTick(callback: (tick: TelemetryTick) => void) {
    tickListeners.push(callback);
    return () => {
      const idx = tickListeners.indexOf(callback);
      if (idx !== -1) tickListeners.splice(idx, 1);
    };
  }

  function isSocketReady(): boolean {
    return !!socket && (socket.readyState === 1 || socket.readyState === (typeof WebSocket !== 'undefined' ? WebSocket.OPEN : 1));
  }

  function connect(customUrl?: string) {
    if (socket && (socket.readyState === 1 || socket.readyState === 0)) {
      return;
    }

    const host = typeof window !== 'undefined' ? (window.location.host || 'localhost:8080') : 'localhost:8080';
    const protocol = typeof window !== 'undefined' && window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const url = customUrl || `${protocol}//${host}/ws/v1/telemetry/live`;

    try {
      socket = new WebSocket(url);

      const handleOpen = () => {
        isConnected.value = true;
        if (activeSubscribedSensors.value.length > 0) {
          sendSubscriptionPayload('subscribe', activeSubscribedSensors.value);
        }
      };

      const handleMessage = (event: MessageEvent) => {
        try {
          const tick: TelemetryTick = typeof event.data === 'string'
            ? JSON.parse(event.data)
            : event.data;

          lastTick.value = tick;
          tickCount.value++;

          if (isLiveEnabled.value) {
            for (const listener of tickListeners) {
              listener(tick);
            }
          }
        } catch (err) {
          console.warn('Failed to parse incoming WebSocket telemetry tick:', err);
        }
      };

      const handleClose = () => {
        isConnected.value = false;
      };

      const handleError = (err: Event) => {
        console.warn('WebSocket telemetry tail error:', err);
        isConnected.value = false;
      };

      socket.addEventListener('open', handleOpen);
      socket.addEventListener('message', handleMessage);
      socket.addEventListener('close', handleClose);
      socket.addEventListener('error', handleError);

      // Support property-based assignment for environments without addEventListener dispatching
      (socket as any).onopen = handleOpen;
      (socket as any).onmessage = handleMessage;
      (socket as any).onclose = handleClose;
      (socket as any).onerror = handleError;
    } catch (e) {
      console.warn('Failed to establish WebSocket connection:', e);
    }
  }

  function sendSubscriptionPayload(action: 'subscribe' | 'unsubscribe', sensorIds: number[]) {
    if (isSocketReady()) {
      socket!.send(JSON.stringify({ action, sensorIds }));
    }
  }

  function subscribe(sensorIds: number[]) {
    activeSubscribedSensors.value = Array.from(new Set([...activeSubscribedSensors.value, ...sensorIds]));
    sendSubscriptionPayload('subscribe', sensorIds);
  }

  function unsubscribe(sensorIds: number[]) {
    activeSubscribedSensors.value = activeSubscribedSensors.value.filter(id => !sensorIds.includes(id));
    sendSubscriptionPayload('unsubscribe', sensorIds);
  }

  function disconnect() {
    if (socket) {
      socket.close();
      socket = null;
    }
    isConnected.value = false;
  }

  return {
    isConnected,
    tickCount,
    lastTick,
    isLiveEnabled,
    activeSubscribedSensors,
    connect,
    subscribe,
    unsubscribe,
    disconnect,
    onTick
  };
}
