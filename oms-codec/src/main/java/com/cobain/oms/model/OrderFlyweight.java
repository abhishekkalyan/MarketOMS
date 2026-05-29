package com.cobain.oms.model;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

/**
 * Zero-allocation flyweight for reading and writing Order fields over a raw binary buffer.
 *
 * ┌─────────────────────────────────────────────────────────────────────────┐
 * │  ZERO-GC GUARANTEE                                                      │
 * │  This object is allocated ONCE (at class construction time) and then    │
 * │  re-pointed at successive buffer slices via wrap(). It never allocates  │
 * │  sub-objects, boxing wrappers, or Strings on the hot path.              │
 * │  All get/set methods are single-instruction buffer reads/writes.        │
 * └─────────────────────────────────────────────────────────────────────────┘
 *
 * Typical lifecycle (single-threaded execution loop):
 *
 *   // At startup — allocate ONE flyweight instance per logical thread.
 *   private final OrderFlyweight flyweight = new OrderFlyweight();
 *
 *   // On each message — re-wrap to point at the correct slot in the OrderBook:
 *   flyweight.wrap(orderBook.buffer(), orderBook.offsetForSlot(slot));
 *   long clOrdId = flyweight.clOrdId();        // zero-alloc read
 *   flyweight.orderState(OrderState.NEW);       // zero-alloc write
 *
 * The flyweight holds only two fields itself: a buffer reference (one pointer) and an
 * integer offset. It is NOT thread-safe; the OMS runs a single-threaded execution loop
 * so thread safety is handled at the architecture level, not the object level.
 */
public final class OrderFlyweight {

    // The single mutable buffer this flyweight is currently pointing at.
    // Using MutableDirectBuffer (extends DirectBuffer) allows both reads and writes.
    private MutableDirectBuffer buffer;
    private int offset;

    // ── Wrapping ──────────────────────────────────────────────────────────────

    /**
     * Re-point this flyweight at [buffer, offset]. Returns {@code this} for
     * fluent chaining: {@code flyweight.wrap(buf, off).clOrdId()}.
     * ZERO-GC: assigns two primitives, no allocation.
     */
    public OrderFlyweight wrap(final MutableDirectBuffer buffer, final int offset) {
        this.buffer = buffer;
        this.offset = offset;
        return this;
    }

    /** Convenience: wrap at offset 0. */
    public OrderFlyweight wrap(final MutableDirectBuffer buffer) {
        return wrap(buffer, 0);
    }

    // ── Getters: direct buffer reads, single CPU instruction each ─────────────

    public long accountId()   { return buffer.getLong(offset + OrderLayout.ACCOUNT_ID_OFFSET); }
    public long clOrdId()     { return buffer.getLong(offset + OrderLayout.CL_ORD_ID_OFFSET); }
    public long orderId()     { return buffer.getLong(offset + OrderLayout.ORDER_ID_OFFSET); }
    public long origClOrdId() { return buffer.getLong(offset + OrderLayout.ORIG_CL_ORD_ID_OFFSET); }
    public long symbol()      { return buffer.getLong(offset + OrderLayout.SYMBOL_OFFSET); }

    /**
     * Price in fixed-point longs (x PRICE_MULTIPLIER = 10,000).
     * e.g. £12.3456 is stored and returned as 123456L.
     * Divide by OrderLayout.PRICE_MULTIPLIER only at display/logging time.
     */
    public long price()       { return buffer.getLong(offset + OrderLayout.PRICE_OFFSET); }
    public long qty()         { return buffer.getLong(offset + OrderLayout.QTY_OFFSET); }
    public long filledQty()   { return buffer.getLong(offset + OrderLayout.FILLED_QTY_OFFSET); }
    public long leavesQty()   { return buffer.getLong(offset + OrderLayout.LEAVES_QTY_OFFSET); }
    public byte side()        { return buffer.getByte(offset + OrderLayout.SIDE_OFFSET); }
    public byte timeInForce() { return buffer.getByte(offset + OrderLayout.TIME_IN_FORCE_OFFSET); }
    public byte orderState()  { return buffer.getByte(offset + OrderLayout.ORDER_STATE_OFFSET); }
    public int  venueId()     { return buffer.getInt(offset + OrderLayout.VENUE_ID_OFFSET); }

    // ── Setters: direct buffer writes ─────────────────────────────────────────

    public void accountId(final long v)   { buffer.putLong(offset + OrderLayout.ACCOUNT_ID_OFFSET, v); }
    public void clOrdId(final long v)     { buffer.putLong(offset + OrderLayout.CL_ORD_ID_OFFSET, v); }
    public void orderId(final long v)     { buffer.putLong(offset + OrderLayout.ORDER_ID_OFFSET, v); }
    public void origClOrdId(final long v) { buffer.putLong(offset + OrderLayout.ORIG_CL_ORD_ID_OFFSET, v); }
    public void symbol(final long v)      { buffer.putLong(offset + OrderLayout.SYMBOL_OFFSET, v); }
    public void price(final long v)       { buffer.putLong(offset + OrderLayout.PRICE_OFFSET, v); }
    public void qty(final long v)         { buffer.putLong(offset + OrderLayout.QTY_OFFSET, v); }
    public void filledQty(final long v)   { buffer.putLong(offset + OrderLayout.FILLED_QTY_OFFSET, v); }
    public void leavesQty(final long v)   { buffer.putLong(offset + OrderLayout.LEAVES_QTY_OFFSET, v); }
    public void side(final byte v)        { buffer.putByte(offset + OrderLayout.SIDE_OFFSET, v); }
    public void timeInForce(final byte v) { buffer.putByte(offset + OrderLayout.TIME_IN_FORCE_OFFSET, v); }
    public void orderState(final byte v)  { buffer.putByte(offset + OrderLayout.ORDER_STATE_OFFSET, v); }
    public void venueId(final int v)      { buffer.putInt(offset + OrderLayout.VENUE_ID_OFFSET, v); }

    // ── Extended field accessors (parent-child linkage, offsets 80–127) ─────────

    public long transactTime()            { return buffer.getLong(offset + OrderLayout.TRANSACT_TIME_OFFSET); }
    public void transactTime(final long v){ buffer.putLong(offset + OrderLayout.TRANSACT_TIME_OFFSET, v); }

    public int  childCount()              { return buffer.getInt(offset + OrderLayout.OFFSET_CHILD_COUNT); }
    public void childCount(final int v)   { buffer.putInt(offset + OrderLayout.OFFSET_CHILD_COUNT, v); }

    public int  nextSiblingSlot()         { return buffer.getInt(offset + OrderLayout.OFFSET_NEXT_SIBLING_SLOT); }
    public void nextSiblingSlot(final int v) { buffer.putInt(offset + OrderLayout.OFFSET_NEXT_SIBLING_SLOT, v); }

    public long parentOrFirstChildId()    { return buffer.getLong(offset + OrderLayout.OFFSET_PARENT_OR_FIRST_CHILD_ID); }
    public void parentOrFirstChildId(final long v) { buffer.putLong(offset + OrderLayout.OFFSET_PARENT_OR_FIRST_CHILD_ID, v); }

    // ── Composite helpers ──────────────────────────────────────────────────────

    /**
     * Atomically update cumulative fill quantities.
     * Called when an EXEC_PARTIAL_FILL or EXEC_FILL ExecReport arrives.
     * All arithmetic is in longs — zero allocation, no BigDecimal.
     *
     * @param lastQty the execution quantity from this single fill event
     */
    public void applyFill(final long lastQty) {
        final long newFilledQty  = filledQty() + lastQty;
        final long newLeavesQty  = qty() - newFilledQty;
        filledQty(newFilledQty);
        leavesQty(Math.max(0L, newLeavesQty)); // clamp to zero on final fill
    }

    /**
     * Copy all ORDER_SIZE bytes from a source DirectBuffer into this flyweight's buffer.
     * Used when persisting a newly received order into the OrderBook backing array.
     * A single memcpy of 80 bytes is the only "copy" in the hot path.
     */
    public void copyFrom(final DirectBuffer src, final int srcOffset) {
        buffer.putBytes(offset, src, srcOffset, OrderLayout.MESSAGE_SIZE);
    }

    /** Exposes the raw buffer reference — needed for snapshot serialization. */
    public MutableDirectBuffer buffer() { return buffer; }

    /** Exposes the current slot offset — needed for snapshot serialization. */
    public int offset() { return offset; }

    /**
     * Encodes an ASCII ticker string (up to 8 chars) into a long by packing each char
     * into one byte, left-aligned, right-padded with spaces (0x20).
     * This is the canonical way to store a FIX Symbol (tag 55) as a primitive long.
     * Only called during order creation — never on the repetitive execution loop.
     */
    public static long encodeSymbol(final String symbol) {
        long encoded = 0x2020202020202020L; // 8 ASCII spaces
        final int len = Math.min(symbol.length(), 8);
        for (int i = 0; i < len; i++) {
            final long ch = symbol.charAt(i) & 0xFFL;
            encoded = (encoded & ~(0xFFL << ((7 - i) * 8))) | (ch << ((7 - i) * 8));
        }
        return encoded;
    }

    /**
     * Decodes a symbol long back to a trimmed ASCII String.
     * Off hot path — allocates a String — diagnostics/logging ONLY.
     */
    public static String decodeSymbol(final long encoded) {
        final byte[] bytes = new byte[8];
        for (int i = 0; i < 8; i++) {
            bytes[i] = (byte) ((encoded >>> ((7 - i) * 8)) & 0xFF);
        }
        return new String(bytes).trim();
    }

    /** Off-path diagnostic representation — allocates a String. */
    @Override
    public String toString() {
        return "Order{clOrdId=" + clOrdId()
               + ", orderId=" + orderId()
               + ", symbol=" + decodeSymbol(symbol())
               + ", side=" + Side.nameOf(side())
               + ", price=" + price() + "(x10000)"
               + ", qty=" + qty()
               + ", filled=" + filledQty()
               + ", state=" + OrderState.nameOf(orderState())
               + '}';
    }
}
