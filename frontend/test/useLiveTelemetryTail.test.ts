import { describe, it, expect, vi, beforeEach } from 'vitest';
import { useLiveTelemetryTail } from '../src/composables/useLiveTelemetryTail';

describe('useLiveTelemetryTail composable', () => {
  let mockWsInstance: any;

  beforeEach(() => {
    vi.restoreAllMocks();

    mockWsInstance = {
      readyState: 1, // OPEN
      send: vi.fn(),
      close: vi.fn(),
      addEventListener: vi.fn((event: string, handler: any) => {
        mockWsInstance[`on${event}`] = handler;
      }),
      removeEventListener: vi.fn()
    };

    // @ts-expect-error mock WebSocket
    globalThis.WebSocket = vi.fn().mockImplementation(() => mockWsInstance);
  });

  it('connects to WebSocket and dispatches subscription payload', () => {
    const { connect, subscribe, isConnected } = useLiveTelemetryTail();

    connect('ws://localhost:8080/ws/v1/telemetry/live');

    // Trigger open
    mockWsInstance.onopen?.(new Event('open'));
    expect(isConnected.value).toBe(true);

    subscribe([1, 2, 3]);

    expect(mockWsInstance.send).toHaveBeenCalledWith(
      JSON.stringify({ action: 'subscribe', sensorIds: [1, 2, 3] })
    );
  });

  it('receives live ticks and invokes listeners', () => {
    const { connect, onTick, tickCount } = useLiveTelemetryTail();
    connect();

    const receivedTicks: any[] = [];
    onTick(tick => receivedTicks.push(tick));

    const sampleTick = {
      sensorId: 1,
      timestampMs: 1729728005000,
      value: 42.15
    };

    // Simulate incoming message
    mockWsInstance.onmessage?.({
      data: JSON.stringify(sampleTick)
    });

    expect(tickCount.value).toBe(1);
    expect(receivedTicks).toHaveLength(1);
    expect(receivedTicks[0]).toEqual(sampleTick);
  });

  it('cleans up connection when disconnect is called', () => {
    const { connect, disconnect, isConnected } = useLiveTelemetryTail();
    connect();

    disconnect();

    expect(mockWsInstance.close).toHaveBeenCalled();
    expect(isConnected.value).toBe(false);
  });
});
