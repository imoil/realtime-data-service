package com.telemetry.engine.window;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.AbstractList;
import java.util.List;

/**
 * Packed binary circular buffer storing historical double values for sliding window evaluations.
 * Provides zero-allocation AbstractList view for AviatorScript functions.
 * State structure:
 * - short capacity (2 bytes)
 * - short count (2 bytes)
 * - int head (4 bytes)
 * - long lastTimestampMs (8 bytes)
 * - double[capacity] values (capacity * 8 bytes)
 */
public class CompactCircularDoubleBuffer {

    public static final int HEADER_SIZE = 16;

    private final short capacity;
    private short count;
    private int head;
    private long lastTimestampMs;
    private final byte[] rawBytes;
    private final ByteBuffer buffer;

    public CompactCircularDoubleBuffer(int capacity) {
        if (capacity <= 0 || capacity > 32767) {
            throw new IllegalArgumentException("Capacity must be between 1 and 32767");
        }
        this.capacity = (short) capacity;
        this.count = 0;
        this.head = -1;
        this.lastTimestampMs = 0L;
        this.rawBytes = new byte[HEADER_SIZE + this.capacity * Double.BYTES];
        this.buffer = ByteBuffer.wrap(this.rawBytes).order(ByteOrder.LITTLE_ENDIAN);
        writeHeader();
    }

    private CompactCircularDoubleBuffer(byte[] bytes) {
        if (bytes == null || bytes.length < HEADER_SIZE) {
            throw new IllegalArgumentException("Invalid byte buffer length");
        }
        this.rawBytes = bytes;
        this.buffer = ByteBuffer.wrap(this.rawBytes).order(ByteOrder.LITTLE_ENDIAN);
        this.capacity = buffer.getShort(0);
        this.count = buffer.getShort(2);
        this.head = buffer.getInt(4);
        this.lastTimestampMs = buffer.getLong(8);
    }

    public static CompactCircularDoubleBuffer fromBytes(byte[] bytes) {
        if (bytes == null || bytes.length < HEADER_SIZE) {
            return null;
        }
        return new CompactCircularDoubleBuffer(bytes);
    }

    public byte[] toBytes() {
        writeHeader();
        return rawBytes;
    }

    private void writeHeader() {
        buffer.putShort(0, capacity);
        buffer.putShort(2, count);
        buffer.putInt(4, head);
        buffer.putLong(8, lastTimestampMs);
    }

    public void add(double value, long timestampMs) {
        int index = (head + 1) % capacity;
        buffer.putDouble(HEADER_SIZE + index * Double.BYTES, value);
        head = index;
        if (count < capacity) {
            count++;
        }
        lastTimestampMs = timestampMs;
    }

    public int getCapacity() {
        return capacity;
    }

    public int getCount() {
        return count;
    }

    public int getHead() {
        return head;
    }

    public long getLastTimestampMs() {
        return lastTimestampMs;
    }

    /**
     * Unmodifiable zero-copy list view over sliding window elements from oldest to newest.
     *
     * @param requestedSize maximum number of historical elements requested
     * @return lightweight List view backed directly by internal byte buffer
     */
    public List<Double> asListView(int requestedSize) {
        final int available = Math.min(requestedSize, (int) count);
        if (available <= 0) {
            return List.of();
        }

        final int startOffset = (head - available + 1 + capacity) % capacity;

        return new AbstractList<Double>() {
            @Override
            public Double get(int index) {
                if (index < 0 || index >= available) {
                    throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + available);
                }
                int circularIdx = (startOffset + index) % capacity;
                return buffer.getDouble(HEADER_SIZE + circularIdx * Double.BYTES);
            }

            @Override
            public int size() {
                return available;
            }
        };
    }

    /**
     * Running average calculation over requested window size.
     */
    public double computeAverage(int requestedSize) {
        int available = Math.min(requestedSize, (int) count);
        if (available <= 0) {
            return 0.0;
        }

        double sum = 0.0;
        int startOffset = (head - available + 1 + capacity) % capacity;
        for (int i = 0; i < available; i++) {
            int circularIdx = (startOffset + i) % capacity;
            sum += buffer.getDouble(HEADER_SIZE + circularIdx * Double.BYTES);
        }
        return sum / available;
    }

    /**
     * Running maximum calculation over requested window size.
     */
    public double computeMax(int requestedSize) {
        int available = Math.min(requestedSize, (int) count);
        if (available <= 0) {
            return -Double.MAX_VALUE;
        }

        double max = -Double.MAX_VALUE;
        int startOffset = (head - available + 1 + capacity) % capacity;
        for (int i = 0; i < available; i++) {
            int circularIdx = (startOffset + i) % capacity;
            double val = buffer.getDouble(HEADER_SIZE + circularIdx * Double.BYTES);
            if (val > max) {
                max = val;
            }
        }
        return max;
    }
}
