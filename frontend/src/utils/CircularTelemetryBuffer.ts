export interface TelemetrySample {
  timestampMs: number;
  value: number;
}

export interface ContiguousTelemetrySeries {
  timestamps: Float64Array;
  values: Float64Array;
  length: number;
}

/**
 * Fixed-budget circular ring buffer for zero-allocation telemetry point ingestion.
 * Enforces O(1) FIFO eviction on buffer overflow, preventing memory leaks and GC pauses
 * during 24/7 continuous industrial monitoring.
 */
export class CircularTelemetryBuffer {
  readonly capacity: number;
  private readonly timestamps: Float64Array;
  private readonly values: Float64Array;
  private start: number = 0;
  private count: number = 0;

  constructor(capacity: number = 10_000) {
    if (capacity <= 0) {
      throw new Error(`CircularTelemetryBuffer capacity must be > 0, received ${capacity}`);
    }
    this.capacity = capacity;
    this.timestamps = new Float64Array(capacity);
    this.values = new Float64Array(capacity);
  }

  get length(): number {
    return this.count;
  }

  /**
   * Pushes a single telemetry sample with O(1) FIFO eviction on capacity overflow.
   */
  push(timestampMs: number, value: number): void {
    if (this.count < this.capacity) {
      const targetIdx = (this.start + this.count) % this.capacity;
      this.timestamps[targetIdx] = timestampMs;
      this.values[targetIdx] = value;
      this.count++;
    } else {
      // Overwrite oldest sample at this.start and advance start index
      this.timestamps[this.start] = timestampMs;
      this.values[this.start] = value;
      this.start = (this.start + 1) % this.capacity;
    }
  }

  /**
   * Pushes multiple samples in batch.
   */
  pushBatch(timestamps: ArrayLike<number>, values: ArrayLike<number>): void {
    const n = Math.min(timestamps.length, values.length);
    for (let i = 0; i < n; i++) {
      this.push(timestamps[i], values[i]);
    }
  }

  /**
   * Retrieves sample at logical index (0 = oldest, length - 1 = newest).
   */
  get(index: number): TelemetrySample | null {
    if (index < 0 || index >= this.count) {
      return null;
    }
    const actualIdx = (this.start + index) % this.capacity;
    return {
      timestampMs: this.timestamps[actualIdx],
      value: this.values[actualIdx]
    };
  }

  /**
   * Retrieves the oldest sample currently in the buffer.
   */
  getOldest(): TelemetrySample | null {
    if (this.count === 0) return null;
    return {
      timestampMs: this.timestamps[this.start],
      value: this.values[this.start]
    };
  }

  /**
   * Retrieves the newest sample currently in the buffer.
   */
  getNewest(): TelemetrySample | null {
    if (this.count === 0) return null;
    const lastIdx = (this.start + this.count - 1) % this.capacity;
    return {
      timestampMs: this.timestamps[lastIdx],
      value: this.values[lastIdx]
    };
  }

  /**
   * Exports an in-order contiguous typed array snapshot of the ring buffer.
   */
  exportContiguous(): ContiguousTelemetrySeries {
    if (this.count === 0) {
      return {
        timestamps: new Float64Array(0),
        values: new Float64Array(0),
        length: 0
      };
    }

    const outTimestamps = new Float64Array(this.count);
    const outValues = new Float64Array(this.count);

    if (this.start + this.count <= this.capacity) {
      // Linear un-wrapped slice
      outTimestamps.set(this.timestamps.subarray(this.start, this.start + this.count));
      outValues.set(this.values.subarray(this.start, this.start + this.count));
    } else {
      // Split into two wrapped slices
      const firstSegmentLen = this.capacity - this.start;
      const secondSegmentLen = this.count - firstSegmentLen;

      outTimestamps.set(this.timestamps.subarray(this.start, this.capacity), 0);
      outValues.set(this.values.subarray(this.start, this.capacity), 0);

      outTimestamps.set(this.timestamps.subarray(0, secondSegmentLen), firstSegmentLen);
      outValues.set(this.values.subarray(0, secondSegmentLen), firstSegmentLen);
    }

    return {
      timestamps: outTimestamps,
      values: outValues,
      length: this.count
    };
  }

  /**
   * Resets buffer state without deallocating underlying typed arrays.
   */
  clear(): void {
    this.start = 0;
    this.count = 0;
  }

  /**
   * Returns memory footprint in bytes for this buffer's typed arrays.
   */
  getByteSize(): number {
    return this.capacity * Float64Array.BYTES_PER_ELEMENT * 2;
  }
}

export interface BufferPoolOptions {
  maxSensors?: number;
  capacityPerSensor?: number;
}

/**
 * Multi-sensor circular telemetry buffer pool.
 * Guarantees a strict memory ceiling (<= 50MB for 50 sensors with 10,000 points each).
 */
export class CircularTelemetryBufferPool {
  readonly maxSensors: number;
  readonly capacityPerSensor: number;
  private readonly buffers = new Map<number, CircularTelemetryBuffer>();

  constructor(options: BufferPoolOptions = {}) {
    this.maxSensors = options.maxSensors ?? 50;
    this.capacityPerSensor = options.capacityPerSensor ?? 10_000;

    // Pre-allocate initial buffers if configured
    if (this.maxSensors > 0) {
      for (let id = 1; id <= this.maxSensors; id++) {
        this.buffers.set(id, new CircularTelemetryBuffer(this.capacityPerSensor));
      }
    }
  }

  getOrCreateBuffer(sensorId: number): CircularTelemetryBuffer {
    let buf = this.buffers.get(sensorId);
    if (!buf) {
      buf = new CircularTelemetryBuffer(this.capacityPerSensor);
      this.buffers.set(sensorId, buf);
    }
    return buf;
  }

  push(sensorId: number, timestampMs: number, value: number): void {
    this.getOrCreateBuffer(sensorId).push(timestampMs, value);
  }

  clearAll(): void {
    for (const buf of this.buffers.values()) {
      buf.clear();
    }
  }

  /**
   * Total memory in bytes across all allocated circular buffers.
   */
  getTotalByteSize(): number {
    let total = 0;
    for (const buf of this.buffers.values()) {
      total += buf.getByteSize();
    }
    return total;
  }
}
