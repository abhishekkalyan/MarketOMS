package com.cobain.oms.codec;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Zero-allocation flyweight for the ChildOrderIntent wire message.
 *
 * Published by algo-sor → oms-core on Aeron IPC stream STREAM_CHILD_INTENTS.
 * oms-core validates and acts on the intent before creating any child order state.
 *
 * Binary layout — 56 bytes total, fits in one cache line:
 *
 *   [  0]  parentOrderId        : long  8 bytes — oms-core-assigned orderId of the parent
 *   [  8]  parentClOrdId        : long  8 bytes — client-assigned clOrdId of the parent
 *   [ 16]  sliceQty             : long  8 bytes — requested child order quantity
 *   [ 24]  limitPrice           : long  8 bytes — limit price (fixed-point × PRICE_SCALE)
 *   [ 32]  venueId              : int   4 bytes — target venue identifier
 *   [ 36]  algoType             : byte  1 byte  — ALGO_SOR / ALGO_ICEBERG / ALGO_TWAP
 *   [ 37]  sliceIndex           : byte  1 byte  — 0-based slice counter (wraps at 255)
 *   [ 38]  _pad                 : short 2 bytes — alignment padding
 *   [ 40]  intentTimestampNanos : long  8 bytes — algo-sor wall-clock nanos at computation time
 *   [ 48]  _reserved            : long  8 bytes — reserved (zero-fill)
 *   [ 56]  — END (BLOCK_LENGTH)
 *
 * ZERO-ALLOCATION: no method on the hot path creates any object. The toString()
 * method allocates a String and must never be called on the hot path.
 */
public final class ChildOrderIntentFlyweight {

    // ── Field offsets ─────────────────────────────────────────────────────────
    private static final int OFFSET_PARENT_ORDER_ID         = 0;
    private static final int OFFSET_PARENT_CL_ORD_ID        = 8;
    private static final int OFFSET_SLICE_QTY               = 16;
    private static final int OFFSET_LIMIT_PRICE             = 24;
    private static final int OFFSET_VENUE_ID                = 32;
    private static final int OFFSET_ALGO_TYPE               = 36;
    private static final int OFFSET_SLICE_INDEX             = 37;
    // 38–39: padding
    private static final int OFFSET_INTENT_TIMESTAMP_NANOS  = 40;
    // 48–55: reserved

    /** Total encoded size in bytes. */
    public static final int BLOCK_LENGTH = 56;

    // ── algoType constants ────────────────────────────────────────────────────
    public static final byte ALGO_SOR     = 1;
    public static final byte ALGO_ICEBERG = 2;
    public static final byte ALGO_TWAP    = 3;

    // ── Buffer reference ──────────────────────────────────────────────────────
    private DirectBuffer buffer;
    private int          baseOffset;

    // ── Wrapping ──────────────────────────────────────────────────────────────

    /**
     * Point this flyweight at a writable buffer slice.
     * ZERO-GC: two field assignments.
     */
    public ChildOrderIntentFlyweight wrap(final MutableDirectBuffer buf, final int offset) {
        this.buffer     = buf;
        this.baseOffset = offset;
        return this;
    }

    /**
     * Point this flyweight at a read-only buffer slice.
     * ZERO-GC: two field assignments.
     */
    public ChildOrderIntentFlyweight wrapReadOnly(final DirectBuffer buf, final int offset) {
        this.buffer     = buf;
        this.baseOffset = offset;
        return this;
    }

    // ── Getters ───────────────────────────────────────────────────────────────

    public long getParentOrderId()         { return buffer.getLong(baseOffset + OFFSET_PARENT_ORDER_ID); }
    public long getParentClOrdId()         { return buffer.getLong(baseOffset + OFFSET_PARENT_CL_ORD_ID); }
    public long getSliceQty()              { return buffer.getLong(baseOffset + OFFSET_SLICE_QTY); }
    public long getLimitPrice()            { return buffer.getLong(baseOffset + OFFSET_LIMIT_PRICE); }
    public int  getVenueId()               { return buffer.getInt(baseOffset  + OFFSET_VENUE_ID); }
    public byte getAlgoType()              { return buffer.getByte(baseOffset  + OFFSET_ALGO_TYPE); }
    public byte getSliceIndex()            { return buffer.getByte(baseOffset  + OFFSET_SLICE_INDEX); }
    public long getIntentTimestampNanos()  { return buffer.getLong(baseOffset + OFFSET_INTENT_TIMESTAMP_NANOS); }

    // ── Setters (require writable buffer) ─────────────────────────────────────

    public void setParentOrderId(final long v)        { ((MutableDirectBuffer) buffer).putLong(baseOffset + OFFSET_PARENT_ORDER_ID, v); }
    public void setParentClOrdId(final long v)        { ((MutableDirectBuffer) buffer).putLong(baseOffset + OFFSET_PARENT_CL_ORD_ID, v); }
    public void setSliceQty(final long v)             { ((MutableDirectBuffer) buffer).putLong(baseOffset + OFFSET_SLICE_QTY, v); }
    public void setLimitPrice(final long v)           { ((MutableDirectBuffer) buffer).putLong(baseOffset + OFFSET_LIMIT_PRICE, v); }
    public void setVenueId(final int v)               { ((MutableDirectBuffer) buffer).putInt(baseOffset  + OFFSET_VENUE_ID, v); }
    public void setAlgoType(final byte v)             { ((MutableDirectBuffer) buffer).putByte(baseOffset + OFFSET_ALGO_TYPE, v); }
    public void setSliceIndex(final byte v)           { ((MutableDirectBuffer) buffer).putByte(baseOffset + OFFSET_SLICE_INDEX, v); }
    public void setIntentTimestampNanos(final long v) { ((MutableDirectBuffer) buffer).putLong(baseOffset + OFFSET_INTENT_TIMESTAMP_NANOS, v); }

    /**
     * Copy the full 56-byte intent record into a destination buffer.
     * Used by AlgoSorAgent to stage the intent into the Aeron offer buffer.
     * ZERO-GC: single bulk memory copy.
     */
    public void copyTo(final MutableDirectBuffer dest, final int destOffset) {
        dest.putBytes(destOffset, buffer, baseOffset, BLOCK_LENGTH);
    }

    /**
     * Pre-allocate a flyweight backed by its own 56-byte UnsafeBuffer.
     * Use this in constructors to create the per-instance pre-allocated intent.
     */
    public static ChildOrderIntentFlyweight allocate() {
        final ChildOrderIntentFlyweight f = new ChildOrderIntentFlyweight();
        final UnsafeBuffer buf = new UnsafeBuffer(new byte[BLOCK_LENGTH]);
        f.wrap(buf, 0);
        return f;
    }

    /** Off-hot-path diagnostic string — allocates. Never call on the hot path. */
    @Override
    public String toString() {
        return "ChildOrderIntent{parentOrderId=" + getParentOrderId()
               + ", parentClOrdId=" + getParentClOrdId()
               + ", sliceQty=" + getSliceQty()
               + ", limitPrice=" + getLimitPrice()
               + ", venueId=" + getVenueId()
               + ", algoType=" + getAlgoType()
               + ", sliceIndex=" + (getSliceIndex() & 0xFF)
               + '}';
    }
}
