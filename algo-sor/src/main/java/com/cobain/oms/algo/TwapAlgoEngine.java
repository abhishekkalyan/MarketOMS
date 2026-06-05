package com.cobain.oms.algo;

import com.cobain.oms.codec.ChildOrderIntentFlyweight;
import com.cobain.oms.model.OrderFlyweight;
import org.agrona.collections.Long2LongHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Time-Weighted Average Price (TWAP) algorithm execution engine.
 *
 * Strategy:
 *   - Split a parent order into N equal-sized time slices.
 *   - First slice is dispatched immediately via onSlice().
 *   - Subsequent slices are dispatched when onTimer() fires.
 *
 * In the intent-driven architecture this engine is a PURE COMPUTATION ENGINE.
 * It creates NO child order state and has NO OrderBook dependency.
 * The stored ChildIntentSink is set during onSlice() and reused by onTimer().
 *
 * ZERO-GC GUARANTEES:
 *   - Per-handle state lives in Agrona Long2LongHashMaps keyed by correlationId.
 *   - The pre-allocated ChildOrderIntentFlyweight is reused for every slice.
 *   - onSlice() and onTimer() dispatch exactly one intent per call.
 *
 * Timer correlation ID layout (64 bits):
 *   [63..32] handle  (int) — identifies the TWAP order
 *   [31.. 0] sliceNo (int) — identifies the slice for idempotency on log replay
 */
public final class TwapAlgoEngine implements AlgoExecutionEngine {

    private static final Logger log = LoggerFactory.getLogger(TwapAlgoEngine.class);

    public static final int   MAX_TWAP_ORDERS    = 512;
    private static final long FREE_HANDLE        = Long.MIN_VALUE;
    private static final int  DEFAULT_SLICES     = 12;
    private static final long DEFAULT_INTERVAL_MS = 5 * 60 * 1_000L;

    private final int maxTwapOrders;

    // ── Per-handle state arrays — indexed by handle ───────────────────────────
    private final long[]    parentOrderId;
    private final long[]    parentClOrdId;
    private final long[]    parentPrice;
    private final int[]     parentVenueId;
    private final long[]    sliceQty;
    private final int[]     totalSlices;
    private final int[]     slicesFired;
    private final long[]    sliceIntervalMs;
    private final boolean[] handleActive;

    // ── Free-handle stack (O(1) alloc/free) ───────────────────────────────────
    private final int[] freeHandles;
    private int         freeHandleTop;

    // ── Pre-allocated intent + stored sink ────────────────────────────────────
    private final ChildOrderIntentFlyweight intent;

    /**
     * Stored sink: set in onSlice(), reused in onTimer().
     * Single-threaded — safe because AlgoSorAgent runs one thread.
     */
    private ChildIntentSink storedSink;

    public TwapAlgoEngine(final int maxTwapOrders) {
        this.maxTwapOrders   = maxTwapOrders;
        this.parentOrderId   = new long[maxTwapOrders];
        this.parentClOrdId   = new long[maxTwapOrders];
        this.parentPrice     = new long[maxTwapOrders];
        this.parentVenueId   = new int[maxTwapOrders];
        this.sliceQty        = new long[maxTwapOrders];
        this.totalSlices     = new int[maxTwapOrders];
        this.slicesFired     = new int[maxTwapOrders];
        this.sliceIntervalMs = new long[maxTwapOrders];
        this.handleActive    = new boolean[maxTwapOrders];
        this.freeHandles     = new int[maxTwapOrders];
        this.intent          = ChildOrderIntentFlyweight.allocate();
        freeHandleTop = maxTwapOrders;
        for (int i = 0; i < maxTwapOrders; i++) {
            freeHandles[i] = i;
        }
    }

    public TwapAlgoEngine() {
        this(MAX_TWAP_ORDERS);
    }

    // ── AlgoExecutionEngine interface ─────────────────────────────────────────

    /**
     * Register a new TWAP order, store the sink, and dispatch the first slice immediately.
     * HOT PATH — array writes, one sink call.
     */
    @Override
    public int onSlice(final OrderFlyweight parent, final ChildIntentSink sink, final long nowNanos) {
        this.storedSink = sink;

        final int handle = acquireHandle();
        if (handle < 0) {
            log.warn("TWAP capacity exhausted (maxTwapOrders={}): orderId={} dispatched as single slice",
                     maxTwapOrders, parent.orderId());
            dispatchSlice(parent.orderId(), parent.clOrdId(), parent.price(),
                          parent.venueId() > 0 ? parent.venueId() : 1,
                          parent.leavesQty(), (byte) 0, sink, nowNanos);
            return 1;
        }

        final long totalQty    = parent.qty();
        final int  numSlices   = DEFAULT_SLICES;
        final long perSliceQty = Math.max(1L, totalQty / numSlices);

        parentOrderId[handle]   = parent.orderId();
        parentClOrdId[handle]   = parent.clOrdId();
        parentPrice[handle]     = parent.price();
        parentVenueId[handle]   = parent.venueId() > 0 ? parent.venueId() : 1;
        sliceQty[handle]        = perSliceQty;
        totalSlices[handle]     = numSlices;
        slicesFired[handle]     = 0;
        sliceIntervalMs[handle] = DEFAULT_INTERVAL_MS;
        handleActive[handle]    = true;

        // Dispatch first slice immediately
        dispatchSlice(parentOrderId[handle], parentClOrdId[handle],
                      parentPrice[handle], parentVenueId[handle],
                      perSliceQty, (byte) 0, sink, nowNanos);
        slicesFired[handle] = 1;

        return 1;
    }

    /**
     * Timer event — dispatch the next TWAP slice.
     * HOT PATH — bit operations, array reads/writes, one sink call.
     */
    @Override
    public void onTimer(final long correlationId, final long timestamp) {
        if (storedSink == null) {
            return;
        }
        final int handle  = (int) (correlationId >>> 32);
        final int sliceNo = (int) (correlationId & 0xFFFFFFFFL);

        if (handle < 0 || handle >= maxTwapOrders || !handleActive[handle]) {
            return;
        }
        if (slicesFired[handle] != sliceNo) {
            return; // idempotency guard
        }

        final int  remaining     = totalSlices[handle] - slicesFired[handle];
        final long thisSliceQty  = (remaining <= 1)
                ? sliceQty[handle]
                : sliceQty[handle];

        if (thisSliceQty > 0L) {
            dispatchSlice(parentOrderId[handle], parentClOrdId[handle],
                          parentPrice[handle], parentVenueId[handle],
                          thisSliceQty, (byte) (slicesFired[handle] & 0xFF),
                          storedSink, timestamp * 1_000_000L);
        }

        slicesFired[handle]++;
        if (slicesFired[handle] >= totalSlices[handle]) {
            releaseHandle(handle);
        }
    }

    @Override
    public void reset() {
        java.util.Arrays.fill(handleActive, false);
        java.util.Arrays.fill(slicesFired, 0);
        freeHandleTop = maxTwapOrders;
        for (int i = 0; i < maxTwapOrders; i++) {
            freeHandles[i] = i;
        }
        storedSink = null;
    }

    // ── Internal helpers ──────────────────────────────────────────────────────

    private void dispatchSlice(
            final long pOrderId, final long pClOrdId,
            final long price, final int venueId,
            final long qty, final byte sliceIdx,
            final ChildIntentSink sink, final long nowNanos) {

        intent.setParentOrderId(pOrderId);
        intent.setParentClOrdId(pClOrdId);
        intent.setSliceQty(qty);
        intent.setLimitPrice(price);
        intent.setVenueId(venueId);
        intent.setAlgoType(ChildOrderIntentFlyweight.ALGO_TWAP);
        intent.setSliceIndex(sliceIdx);
        intent.setIntentTimestampNanos(nowNanos);

        sink.onIntent(intent);
    }

    private int acquireHandle() {
        if (freeHandleTop == 0) return -1;
        return freeHandles[--freeHandleTop];
    }

    private void releaseHandle(final int handle) {
        handleActive[handle] = false;
        freeHandles[freeHandleTop++] = handle;
    }
}
