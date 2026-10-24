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
}

export function useTelemetryStream() {
  const isLoading = ref(false);
  const error = ref<string | null>(null);
  const isProgrammaticUpdate = ref(false);
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

  async function fetchAm4Arrow(options: FetchOptions): Promise<TelemetryBatch> {
    if (abortController) {
      abortController.abort();
    }
    abortController = new AbortController();
    const signal = abortController.signal;

    isLoading.value = true;
    error.value = null;

    try {
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

      const response = await fetch(`/api/v1/telemetry/am4-arrow?${params.toString()}`, {
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

      // Decode with flechette zero-copy table reader
      const table = tableFromIPC(buffer);
      const rowCount = table.numRows;

      const sensorIdCol = table.getChild('sensor_id');
      const tsMsCol = table.getChild('ts_ms');
      const valCol = table.getChild('val');

      if (!tsMsCol || !valCol) {
        throw new Error('Arrow stream is missing required fields (ts_ms or val)');
      }

      // Zero-copy typed array views directly backed by buffer memory
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
      return batch;
    } catch (err: unknown) {
      if ((err as Error)?.name === 'AbortError') {
        // Ignored as expected on concurrent updates
        throw err;
      }
      const message = (err as Error)?.message ?? 'Unknown streaming failure';
      error.value = message;
      throw err;
    } finally {
      isLoading.value = false;
    }
  }

  return {
    isLoading,
    error,
    isProgrammaticUpdate,
    currentBatch,
    decomposeBySensor,
    fetchAm4Arrow
  };
}
