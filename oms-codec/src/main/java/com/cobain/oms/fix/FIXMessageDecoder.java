package com.cobain.oms.fix;

import com.cobain.oms.model.OrderFlyweight;
import com.cobain.oms.model.OrderLayout;
import org.agrona.DirectBuffer;

/**
 * Zero-copy translation layer: FIX Binary → OMS OrderFlyweight.
 *
 * The upstream FIX connectivity engine pre-parses raw FIX wire bytes into a compact
 * "FIX Binary" format (defined below) and publishes it to an Aeron IPC channel.
 * The OMS receives this buffer via {@code onSessionMessage} and calls this decoder
 * to project the fields into a writable {@link OrderFlyweight} (which points to a
 * slot in the {@link com.cobain.oms.core.OrderBook} backing array).
 *
 * ┌─────────────────────────────────────────────────────────────────────────┐
 * │  FIX BINARY INBOUND FORMAT  (76 bytes)                                  │
 * │                                                                         │
 * │   [0]  msgType      : byte  — FIX MsgType: 'D'=NOS, 'F'=Cancel,       │
 * │                               'G'=Replace, '8'=ExecReport              │
 * │   [1]  side         : byte  — FIX tag 54 (matches Side constants)      │
 * │   [2]  timeInForce  : byte  — FIX tag 59                               │
 * │   [3]  execType     : byte  — FIX tag 150 (for ExecReport only)        │
 * │   [4]  clOrdId      : long  — FIX tag 11, hash-encoded as primitive    │
 * │  [12]  origClOrdId  : long  — FIX tag 41                               │
 * │  [20]  orderId      : long  — FIX tag 37 (venue-assigned)              │
 * │  [28]  accountId    : long  — FIX tag 1                                │
 * │  [36]  symbol       : long  — FIX tag 55, packed ASCII (see encodeSymbol)│
 * │  [44]  price        : long  — FIX tag 44, fixed-point x10000           │
 * │  [52]  orderQty     : long  — FIX tag 38                               │
 * │  [60]  lastQty      : long  — FIX tag 32 (ExecReport fill qty)        │
 * │  [68]  leavesQty    : long  — FIX tag 151                              │
 * │  Total: 76 bytes                                                        │
 * └─────────────────────────────────────────────────────────────────────────┘
 *
 * ZERO-GC: Each method is a sequence of buffer.getLong/getByte calls writing
 * directly into the target flyweight via putLong/putByte.
 * No intermediate Java objects are created; no field-by-field String parsing.
 */
public final class FIXMessageDecoder {

    // ── FIX Binary inbound field offsets ──────────────────────────────────────
    public static final int MSG_TYPE_OFFSET      = 0;
    public static final int SIDE_OFFSET_IN       = 1;
    public static final int TIF_OFFSET_IN        = 2;
    public static final int EXEC_TYPE_OFFSET_IN  = 3;
    public static final int CL_ORD_ID_OFFSET_IN  = 4;
    public static final int ORIG_CL_ORD_ID_IN    = 12;
    public static final int ORDER_ID_OFFSET_IN   = 20;
    public static final int ACCOUNT_ID_OFFSET_IN = 28;
    public static final int SYMBOL_OFFSET_IN     = 36;
    public static final int PRICE_OFFSET_IN      = 44;
    public static final int ORDER_QTY_OFFSET_IN  = 52;
    public static final int LAST_QTY_OFFSET_IN   = 60;
    public static final int LEAVES_QTY_OFFSET_IN = 68;

    /** Total size of one inbound FIX Binary message. */
    public static final int FIX_BINARY_SIZE = 76;

    // ── FIX MsgType constants (ASCII byte values) ─────────────────────────────
    public static final byte MSG_NEW_ORDER_SINGLE = 'D';
    public static final byte MSG_CANCEL_REQUEST   = 'F';
    public static final byte MSG_CANCEL_REPLACE   = 'G';
    public static final byte MSG_EXEC_REPORT      = '8';

    // ── FIX ExecType constants (ASCII byte values, FIX tag 150) ───────────────
    public static final byte EXEC_TYPE_NEW          = '0';
    public static final byte EXEC_TYPE_PARTIAL_FILL = '1';
    public static final byte EXEC_TYPE_FILL         = '2';
    public static final byte EXEC_TYPE_CANCELED     = '4';
    public static final byte EXEC_TYPE_REPLACED     = '5';
    public static final byte EXEC_TYPE_REJECTED     = '8';
    public static final byte EXEC_TYPE_EXPIRED      = 'C';

    // ── Decoding methods — one per inbound message type ────────────────────────

    /**
     * Decode a NewOrderSingle from the FIX Binary buffer into a writable target flyweight.
     *
     * HOT PATH: each statement is a single buffer.getLong/getByte + buffer.putLong/putByte.
     * The JIT will typically inline and eliminate these getter/setter calls entirely.
     *
     * @param src       source buffer containing the FIX Binary message
     * @param srcOffset byte offset within src where the FIX Binary message starts
     * @param target    writable flyweight pointing at a freshly allocated OrderBook slot
     */
    public static void decodeNewOrderSingle(
            final DirectBuffer src,
            final int srcOffset,
            final OrderFlyweight target) {

        // Zero-copy: read each field from the FIX Binary buffer and write to the OMS slot.
        // The target flyweight writes directly into the OrderBook backing array —
        // no intermediate representation, no boxing, no String conversion.
        target.clOrdId(     src.getLong(srcOffset + CL_ORD_ID_OFFSET_IN));
        target.origClOrdId( src.getLong(srcOffset + ORIG_CL_ORD_ID_IN));
        target.orderId(     OrderLayout.NULL_ID);         // not yet assigned
        target.accountId(   src.getLong(srcOffset + ACCOUNT_ID_OFFSET_IN));
        target.symbol(      src.getLong(srcOffset + SYMBOL_OFFSET_IN));
        target.price(       src.getLong(srcOffset + PRICE_OFFSET_IN));
        target.qty(         src.getLong(srcOffset + ORDER_QTY_OFFSET_IN));
        target.filledQty(   0L);
        target.leavesQty(   src.getLong(srcOffset + ORDER_QTY_OFFSET_IN)); // initially = qty
        target.side(        src.getByte(srcOffset + SIDE_OFFSET_IN));
        target.timeInForce( src.getByte(srcOffset + TIF_OFFSET_IN));
        target.orderState(  com.cobain.oms.model.OrderState.PENDING_NEW);
        target.venueId(     0); // default venue; SOR may override
    }

    /**
     * Decode a CancelRequest. Copies identifying fields; qty/price are taken
     * from the existing order in the book (not from the cancel message itself).
     */
    public static void decodeCancelRequest(
            final DirectBuffer src,
            final int srcOffset,
            final OrderFlyweight target) {

        target.clOrdId(     src.getLong(srcOffset + CL_ORD_ID_OFFSET_IN));
        target.origClOrdId( src.getLong(srcOffset + ORIG_CL_ORD_ID_IN));
        target.accountId(   src.getLong(srcOffset + ACCOUNT_ID_OFFSET_IN));
        target.symbol(      src.getLong(srcOffset + SYMBOL_OFFSET_IN));
        target.side(        src.getByte(srcOffset + SIDE_OFFSET_IN));
    }

    /**
     * Decode a CancelReplaceRequest (OrderCancelReplaceRequest, FIX MsgType='G').
     * Carries updated price and qty in addition to the identifying fields.
     */
    public static void decodeCancelReplace(
            final DirectBuffer src,
            final int srcOffset,
            final OrderFlyweight target) {

        target.clOrdId(     src.getLong(srcOffset + CL_ORD_ID_OFFSET_IN));
        target.origClOrdId( src.getLong(srcOffset + ORIG_CL_ORD_ID_IN));
        target.accountId(   src.getLong(srcOffset + ACCOUNT_ID_OFFSET_IN));
        target.symbol(      src.getLong(srcOffset + SYMBOL_OFFSET_IN));
        target.price(       src.getLong(srcOffset + PRICE_OFFSET_IN));
        target.qty(         src.getLong(srcOffset + ORDER_QTY_OFFSET_IN));
        target.side(        src.getByte(srcOffset + SIDE_OFFSET_IN));
        target.timeInForce( src.getByte(srcOffset + TIF_OFFSET_IN));
    }

    /**
     * Decode an ExecutionReport (FIX MsgType='8') from a venue.
     * Fills in the execType return value so the caller can determine the OrderEvent.
     *
     * @return the raw execType byte from the message (use {@code mapExecTypeToEvent})
     */
    public static byte decodeExecReport(
            final DirectBuffer src,
            final int srcOffset,
            final OrderFlyweight target) {

        target.orderId(   src.getLong(srcOffset + ORDER_ID_OFFSET_IN));
        target.clOrdId(   src.getLong(srcOffset + CL_ORD_ID_OFFSET_IN));
        target.leavesQty( src.getLong(srcOffset + LEAVES_QTY_OFFSET_IN));
        // lastQty is not written to the flyweight here; it is returned for fill accounting
        return src.getByte(srcOffset + EXEC_TYPE_OFFSET_IN);
    }

    /**
     * Returns the lastQty from an ExecReport buffer without touching the target flyweight.
     * Called when the exec type is a fill event (PARTIAL or FULL).
     */
    public static long getLastQty(final DirectBuffer src, final int srcOffset) {
        return src.getLong(srcOffset + LAST_QTY_OFFSET_IN);
    }

    /**
     * Maps a FIX ExecType byte to an OMS OrderEvent byte.
     * Single tableswitch — zero allocation, branch-free for JIT inlining.
     *
     * @return the corresponding OrderEvent constant, or -1 if unrecognised
     */
    public static byte mapExecTypeToEvent(final byte execType) {
        return switch (execType) {
            case EXEC_TYPE_NEW          -> com.cobain.oms.model.OrderEvent.EXEC_NEW;
            case EXEC_TYPE_PARTIAL_FILL -> com.cobain.oms.model.OrderEvent.EXEC_PARTIAL_FILL;
            case EXEC_TYPE_FILL         -> com.cobain.oms.model.OrderEvent.EXEC_FILL;
            case EXEC_TYPE_CANCELED     -> com.cobain.oms.model.OrderEvent.EXEC_CANCELED;
            case EXEC_TYPE_REPLACED     -> com.cobain.oms.model.OrderEvent.EXEC_REPLACED;
            case EXEC_TYPE_REJECTED     -> com.cobain.oms.model.OrderEvent.EXEC_REJECTED;
            case EXEC_TYPE_EXPIRED      -> com.cobain.oms.model.OrderEvent.EXEC_EXPIRED;
            default                     -> com.cobain.oms.model.OrderEvent.INVALID_TRANSITION;
        };
    }

    private FIXMessageDecoder() {}
}
