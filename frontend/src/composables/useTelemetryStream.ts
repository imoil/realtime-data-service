import { ref, shallowRef } from 'vue';
import { tableFromIPC } from '@uwdata/flechette';

export interface TelemetryBatch {
  sensorId: Uint32Array;
  tsMs: Float64Array;
  val: Float64Array;
  length: number;
}

export interface SensorSeries {
  sensorId: number;
  tsMs: Float64Array;
  val: Float64Array;
  length: number;
}

export interface FetchOptions {
  sensorId?: number;
  sensorIds?: number[];
  lbSec: number;
  ubSec: number;
  width?: number;
  maxRetries?: number;
  initialDelayMs?: number;
  backoffMultiplier?: number;
}

/**
 * Validates that an Arrow IPC stream buffer contains a valid End-Of-Stream (EOS) marker.
 *
 * In Apache Arrow IPC Stream specification:
 * - Each message begins with a 4-byte continuation indicator (0xFFFFFFFF) and 4-byte length.
 * - An EOS indicator is signaled by:
 *     - Continuation (0xFFFFFFFF) followed by 0x00000000 (last 8 bytes),
 *     - Or a 4-byte 0xFFFFFFFF stream terminator.
 */
export function hasValidArrowEos(buffer: ArrayBuffer | ArrayBufferLike): boolean {
  if (!buffer || buffer.byteLength < 4) {
    return false;
  }
  const view = new DataView(buffer as ArrayBuffer);
  const len = buffer.byteLength;

  // Check 8-byte continuation + 0 length: [0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00, 0x00, 0x00]
  if (len >= 8) {
    const marker = view.getUint32(len - 8, true);
    const zeroLen = view.getUint32(len - 4, true);
    if (marker === 0xFFFFFFFF && zeroLen === 0) {
      return true;
    }
  }

  // Check 4-byte EOS terminator (0xFFFFFFFF)
  const last4 = view.getUint32(len - 4, true);
  if (last4 === 0xFFFFFFFF) {
    return true;
  }

  return false;
}

export function useTelemetryStream() {
  const isLoading = ref(false);
  const error = ref<string | null>(null);
  const isProgrammaticUpdate = ref(false);
  const retryCount = ref(0);
  const currentBatch = shallowRef<TelemetryBatch | null>(null);

  let abortController: AbortController | null = null;

  /**
   * Decomposes a multi-sensor Arrow batch into zero-copy subarray views per sensor.
   * Leverages sorted sensor_id values without allocating new underlying array buffers.
   */
  function decomposeBySensor(batch: TelemetryBatch): Map<number, SensorSeries> {
    const result = new Map<number, SensorSeries>();
    const n = batch.length;
    if (n === 0) return result;

    const sensorIds = batch.sensorId;
    let startIdx = 0;
    let currentSensor = sensorIds[0];

    for (let i = 1; i <= n; i++) {
      if (i === n || sensorIds[i] !== currentSensor) {
        // Zero-copy view slice
        const tsMsSlice = batch.tsMs.subarray(startIdx, i);
        const valSlice = batch.val.subarray(startIdx, i);

        result.set(currentSensor, {
          sensorId: currentSensor,
          tsMs: tsMsSlice,
          val: valSlice,
          length: i - startIdx
        });

        if (i < n) {
          currentSensor = sensorIds[i];
          startIdx = i;
        }
      }
    }

    return result;
  }

  /**
   * Fetches Arrow IPC stream with:
   * 1. AbortController for cancelling in-flight requests on rapid zoom/pan.
   * 2. Arrow EOS marker validation (0xFFFFFFFF).
   * 3. Exponential backoff auto-retry on truncated or malformed streams.
   */
  async function fetchAm4Arrow(options: FetchOptions): Promise<TelemetryBatch> {
    if (abortController) {
      abortController.abort();
    }
    abortController = new AbortController();
    const signal = abortController.signal;

    isLoading.value = true;
    error.value = null;
    retryCount.value = 0;

    const maxRetries = options.maxRetries ?? 3;
    const initialDelayMs = options.initialDelayMs ?? 100;
    const backoffMultiplier = options.backoffMultiplier ?? 2;

    const width = options.width ?? 1920;
    const params = new URLSearchParams({
      lb_sec: options.lbSec.toString(),
      ub_sec: options.ubSec.toString(),
      width: width.toString()
    });

    if (options.sensorIds && options.sensorIds.length > 0) {
      params.set('sensor_ids', options.sensorIds.join(','));
    } else {
      const sensorId = options.sensorId ?? 1;
      params.set('sensor_id', sensorId.toString());
    }

    const url = `/api/v1/telemetry/am4-arrow?${params.toString()}`;

    let lastError: Error | null = null;

    for (let attempt = 0; attempt <= maxRetries; attempt++) {
      if (signal.aborted) {
        const abortErr = new Error('Request aborted');
        abortErr.name = 'AbortError';
        throw abortErr;
      }

      if (attempt > 0) {
        retryCount.value = attempt;
        const delay = initialDelayMs * Math.pow(backoffMultiplier, attempt - 1);
        await new Promise<void>((resolve, reject) => {
          const timeout = setTimeout(resolve, delay);
          signal.addEventListener('abort', () => {
            clearTimeout(timeout);
            const abortErr = new Error('Request aborted');
            abortErr.name = 'AbortError';
            reject(abortErr);
          }, { once: true });
        });
      }

      try {
        const response = await fetch(url, {
          method: 'GET',
          headers: {
            'Accept': 'application/vnd.apache.arrow.stream'
          },
          signal
        });

        if (!response.ok) {
          throw new Error(`HTTP error ${response.status}: ${response.statusText}`);
        }

        const buffer = await response.arrayBuffer();
        if (!buffer || buffer.byteLength < 8) {
          throw new Error('Invalid or truncated Arrow IPC stream: buffer too short');
        }

        // Validate Arrow EOS marker (0xFFFFFFFF)
        if (!hasValidArrowEos(buffer)) {
          throw new Error('Stream truncated or malformed: missing Arrow EOS marker (0xFFFFFFFF)');
        }

        // Decode with Flechette zero-copy table reader
        const table = tableFromIPC(buffer);
        const rowCount = table.numRows;

        const sensorIdCol = table.getChild('sensor_id');
        const tsMsCol = table.getChild('ts_ms');
        const valCol = table.getChild('val');

        if (!tsMsCol || !valCol) {
          throw new Error('Arrow stream is missing required fields (ts_ms or val)');
        }

        const sensorIdArray = sensorIdCol ? (sensorIdCol.toArray() as Uint32Array) : new Uint32Array(rowCount);
        const tsMsArray = tsMsCol.toArray() as Float64Array;
        const valArray = valCol.toArray() as Float64Array;

        const batch: TelemetryBatch = {
          sensorId: sensorIdArray,
          tsMs: tsMsArray,
          val: valArray,
          length: rowCount
        };

        currentBatch.value = batch;
        isLoading.value = false;
        return batch;
      } catch (err: unknown) {
        const errorObj = err as Error;
        if (errorObj?.name === 'AbortError' || signal.aborted) {
          // Do not retry intentionally aborted requests
          throw errorObj;
        }

        lastError = errorObj;
        if (attempt === maxRetries) {
          const message = errorObj?.message ?? 'Unknown streaming failure';
          error.value = message;
          isLoading.value = false;
          throw errorObj;
        }
      }
    }

    isLoading.value = false;
    throw lastError ?? new Error('Streaming failed after max retries');
  }

  return {
    isLoading,
    error,
    isProgrammaticUpdate,
    retryCount,
    currentBatch,
    decomposeBySensor,
    fetchAm4Arrow
  };
}
