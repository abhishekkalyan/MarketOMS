package com.cobain.oms.fix;

import com.cobain.oms.model.OrderFlyweight;
import com.cobain.oms.model.OrderLayout;
import com.cobain.oms.model.OrderState;
import io.aeron.ExclusivePublication;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Zero-copy translation layer: OMS OrderFlyweight → FIX Binary outbound.
 *
 * Encodes an OMS order into the FIX Binary wire format understood by the downstream
 * FIX connectivity engine and offers it to the engine's Aeron IPC publication.
 *
 * FIX Binary Outbound Format (same layout as inbound, defined in FIXMessageDecoder):
 *   Total: 76 bytes, all primitive fields, no strings, no delimiters.
 *
 * ZERO-GC: The encoder holds a single pre-allocated UnsafeBuffer used for all
 * outbound messages. Because the OMS is single-threaded, we never need more than
 * one concurrent outbound buffer.
 */
public final class FIXMessageEncoder {

    /** Pre-allocated outbound buffer — one per encoder instance, never re-allocated. */
    private final UnsafeBuffer outbound =
            new UnsafeBuffer(new byte[FIXMessageDecoder.FIX_BINARY_SIZE]);

    /** Aeron publication targeting the FIX engine's inbound subscription. */
    private final ExclusivePublication fixEnginePublication;

    public FIXMessageEncoder(final ExclusivePublication fixEnginePublication) {
        this.fixEnginePublication = fixEnginePublication;
    }

    // ── Encoding methods ──────────────────────────────────────────────────────

    /**
     * Encode and send a NewOrderSingle to the FIX connectivity engine.
     * HOT PATH — 14 buffer writes + one Aeron offer.
     */
    public long sendNewOrderSingle(final OrderFlyweight order) {
        outbound.putByte(FIXMessageDecoder.MSG_TYPE_OFFSET, FIXMessageDecoder.MSG_NEW_ORDER_SINGLE);
        populateOrderFields(order);
        return offer();
    }

    /**
     * Encode and send an OrderCancelRequest.
     */
    public long sendCancelRequest(final OrderFlyweight order) {
        outbound.putByte(FIXMessageDecoder.MSG_TYPE_OFFSET, FIXMessageDecoder.MSG_CANCEL_REQUEST);
        outbound.putByte(FIXMessageDecoder.SIDE_OFFSET_IN,  order.side());
        outbound.putLong(FIXMessageDecoder.CL_ORD_ID_OFFSET_IN,   order.clOrdId());
        outbound.putLong(FIXMessageDecoder.ORIG_CL_ORD_ID_IN,     order.origClOrdId());
        outbound.putLong(FIXMessageDecoder.ACCOUNT_ID_OFFSET_IN,   order.accountId());
        outbound.putLong(FIXMessageDecoder.SYMBOL_OFFSET_IN,       order.symbol());
        return offer();
    }

    /**
     * Encode and send an OrderCancelReplaceRequest.
     */
    public long sendCancelReplace(final OrderFlyweight order) {
        outbound.putByte(FIXMessageDecoder.MSG_TYPE_OFFSET, FIXMessageDecoder.MSG_CANCEL_REPLACE);
        populateOrderFields(order);
        return offer();
    }

    /**
     * Encode and send an ExecutionReport back to the client-facing FIX engine.
     *
     * @param order     the order to report on
     * @param execType  FIX ExecType byte (e.g. EXEC_TYPE_PARTIAL_FILL)
     * @param lastQty   fill quantity for this event; 0 for non-fill exec reports
     */
    public long sendExecReport(
            final OrderFlyweight order,
            final byte execType,
            final long lastQty) {

        outbound.putByte(FIXMessageDecoder.MSG_TYPE_OFFSET,      FIXMessageDecoder.MSG_EXEC_REPORT);
        outbound.putByte(FIXMessageDecoder.EXEC_TYPE_OFFSET_IN,  execType);
        outbound.putByte(FIXMessageDecoder.SIDE_OFFSET_IN,       order.side());
        outbound.putByte(FIXMessageDecoder.TIF_OFFSET_IN,        order.timeInForce());
        outbound.putLong(FIXMessageDecoder.CL_ORD_ID_OFFSET_IN,  order.clOrdId());
        outbound.putLong(FIXMessageDecoder.ORIG_CL_ORD_ID_IN,    order.origClOrdId());
        outbound.putLong(FIXMessageDecoder.ORDER_ID_OFFSET_IN,   order.orderId());
        outbound.putLong(FIXMessageDecoder.ACCOUNT_ID_OFFSET_IN, order.accountId());
        outbound.putLong(FIXMessageDecoder.SYMBOL_OFFSET_IN,     order.symbol());
        outbound.putLong(FIXMessageDecoder.PRICE_OFFSET_IN,      order.price());
        outbound.putLong(FIXMessageDecoder.ORDER_QTY_OFFSET_IN,  order.qty());
        outbound.putLong(FIXMessageDecoder.LAST_QTY_OFFSET_IN,   lastQty);
        outbound.putLong(FIXMessageDecoder.LEAVES_QTY_OFFSET_IN, order.leavesQty());
        return offer();
    }

    /**
     * Map an OMS OrderState to the corresponding FIX ExecType byte.
     * Used when generating execution reports from state transitions.
     */
    public static byte orderStateToExecType(final byte orderState) {
        return switch (orderState) {
            case OrderState.NEW              -> FIXMessageDecoder.EXEC_TYPE_NEW;
            case OrderState.PARTIALLY_FILLED -> FIXMessageDecoder.EXEC_TYPE_PARTIAL_FILL;
            case OrderState.FILLED           -> FIXMessageDecoder.EXEC_TYPE_FILL;
            case OrderState.CANCELED         -> FIXMessageDecoder.EXEC_TYPE_CANCELED;
            case OrderState.REPLACED         -> FIXMessageDecoder.EXEC_TYPE_REPLACED;
            case OrderState.REJECTED         -> FIXMessageDecoder.EXEC_TYPE_REJECTED;
            case OrderState.EXPIRED          -> FIXMessageDecoder.EXEC_TYPE_EXPIRED;
            default                          -> FIXMessageDecoder.EXEC_TYPE_NEW;
        };
    }

    // ── Internal helpers ──────────────────────────────────────────────────────

    private void populateOrderFields(final OrderFlyweight order) {
        outbound.putByte(FIXMessageDecoder.SIDE_OFFSET_IN,        order.side());
        outbound.putByte(FIXMessageDecoder.TIF_OFFSET_IN,         order.timeInForce());
        outbound.putLong(FIXMessageDecoder.CL_ORD_ID_OFFSET_IN,   order.clOrdId());
        outbound.putLong(FIXMessageDecoder.ORIG_CL_ORD_ID_IN,     order.origClOrdId());
        outbound.putLong(FIXMessageDecoder.ORDER_ID_OFFSET_IN,    order.orderId());
        outbound.putLong(FIXMessageDecoder.ACCOUNT_ID_OFFSET_IN,  order.accountId());
        outbound.putLong(FIXMessageDecoder.SYMBOL_OFFSET_IN,      order.symbol());
        outbound.putLong(FIXMessageDecoder.PRICE_OFFSET_IN,       order.price());
        outbound.putLong(FIXMessageDecoder.ORDER_QTY_OFFSET_IN,   order.qty());
        outbound.putLong(FIXMessageDecoder.LAST_QTY_OFFSET_IN,    0L);
        outbound.putLong(FIXMessageDecoder.LEAVES_QTY_OFFSET_IN,  order.leavesQty());
    }

    /**
     * Non-blocking offer to the FIX engine publication.
     * Returns the publication position on success (≥ 0), or a negative back-pressure code.
     */
    private long offer() {
        return fixEnginePublication.offer(outbound, 0, FIXMessageDecoder.FIX_BINARY_SIZE);
    }
}
