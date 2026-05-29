package com.cobain.oms.algo;

import com.cobain.oms.core.OrderBook;
import com.cobain.oms.model.OrderFlyweight;
import com.cobain.oms.model.OrderLayout;
import com.cobain.oms.model.OrderState;
import io.aeron.cluster.service.Cluster;

/**
 * Time-Weighted Average Price (TWAP) algorithm execution engine.
 *
 * Strategy:
 *   - Split a parent order into N equal-sized time slices.
 *   - Dispatch one slice per interval using Aeron Cluster's deterministic timer mechanism.
 *   - Because timers fire via {@code onTimerEvent} on the Raft log, all replicas advance
 *     time identically — TWAP execution is reproducible on every node during failover.
 *
 * ZERO-GC GUARANTEES:
 *   - TWAP state is held in parallel primitive long/int arrays indexed by a compact
 *     "twap handle" (0..MAX_TWAP_ORDERS-1).
 *   - The correlation ID passed to Cluster.scheduleTimer encodes both the handle and
 *     the slice sequence number, so onTimer() decodes state with two bitwise operations.
 *   - No Map<>, no Iterator, no boxing on any path exercised after onOrder() returns.
 *
 * Timer correlation ID layout (64 bits):
 *   [63..32] handle  (int, 32 bits) — identifies the TWAP order
 *   [31.. 0] sliceNo (int, 32 bits) — identifies the slice (for idempotency on replay)
 */
public final class TwapAlgoEngine implements AlgoExecutionEngine {

    /** Maximum number of concurrently running TWAP orders. */
    private static final int MAX_TWAP_ORDERS = 512;

    /** Sentinel meaning "this handle slot is free". */
    private static final long FREE_HANDLE = 0L;

    // ── Per-handle state arrays ───────────────────────────────────────────────
    private final long[] parentClOrdId    = new long[MAX_TWAP_ORDERS];  // identifies parent order
    private final int[]  parentSlot       = new int[MAX_TWAP_ORDERS];   // slot in OrderBook
    private final long[] sliceQty         = new long[MAX_TWAP_ORDERS];  // qty per slice
    private final int[]  totalSlices      = new int[MAX_TWAP_ORDERS];   // total number of slices
    private final int[]  slicesFired      = new int[MAX_TWAP_ORDERS];   // slices sent so far
    private final long[] sliceIntervalMs  = new long[MAX_TWAP_ORDERS];  // millis between slices

    // handleActive[h] == true while this handle is in use
    private final boolean[] handleActive  = new boolean[MAX_TWAP_ORDERS];

    // ── Free-handle stack (O(1) alloc/free) ───────────────────────────────────
    private final int[] freeHandles = new int[MAX_TWAP_ORDERS];
    private int freeHandleTop;

    // ── Shared references ─────────────────────────────────────────────────────
    private final OrderBook        orderBook;
    private final SmartOrderRouter sor;
    private Cluster                cluster; // injected after onStart()

    /** Default TWAP parameters: slice over 12 intervals spaced 5 minutes apart. */
    private static final int  DEFAULT_SLICES       = 12;
    private static final long DEFAULT_INTERVAL_MS  = 5 * 60 * 1_000L;

    public TwapAlgoEngine(final OrderBook orderBook, final SmartOrderRouter sor) {
        this.orderBook = orderBook;
        this.sor       = sor;

        freeHandleTop = MAX_TWAP_ORDERS;
        for (int i = 0; i < MAX_TWAP_ORDERS; i++) {
            freeHandles[i] = i;
        }
    }

    /**
     * Inject the Cluster reference after onStart() — needed to call scheduleTimer().
     * Called off the hot path, once during startup or leader election.
     */
    public void setCluster(final Cluster cluster) {
        this.cluster = cluster;
    }

    // ── AlgoExecutionEngine interface ─────────────────────────────────────────

    /**
     * Register a new TWAP order and schedule the first slice timer.
     * HOT PATH — array writes, one Cluster.scheduleTimer() call.
     */
    @Override
    public void onOrder(
            final OrderFlyweight parentOrder,
            final int slot,
            final long timestamp) {

        final int handle = acquireHandle();
        if (handle < 0) {
            // TWAP capacity exceeded — fall back to single-shot routing
            sor.routeChildOrder(parentOrder, slot);
            return;
        }

        final long totalQty      = parentOrder.qty();
        final int  numSlices     = DEFAULT_SLICES;
        final long perSliceQty   = Math.max(1L, totalQty / numSlices);
        final long intervalMs    = DEFAULT_INTERVAL_MS;

        parentClOrdId[handle]   = parentOrder.clOrdId();
        parentSlot[handle]      = slot;
        sliceQty[handle]        = perSliceQty;
        totalSlices[handle]     = numSlices;
        slicesFired[handle]     = 0;
        sliceIntervalMs[handle] = intervalMs;
        handleActive[handle]    = true;

        // Schedule the first slice immediately (timestamp + 0) so the first slice fires
        // at the start of the TWAP window, then subsequent slices at regular intervals.
        scheduleNextTimer(handle, timestamp, 0);
    }

    /**
     * Timer event from Aeron Cluster — decode the handle and fire one TWAP slice.
     * This callback is deterministic: every node in the cluster receives the identical
     * event at the identical position in the Raft log, guaranteeing replay fidelity.
     *
     * HOT PATH — bit operations, array reads/writes, one SOR call.
     */
    @Override
    public void onTimer(final long correlationId, final long timestamp) {
        // Decode correlation ID: high 32 bits = handle, low 32 bits = sliceNo
        final int handle  = (int) (correlationId >>> 32);
        final int sliceNo = (int) (correlationId & 0xFFFFFFFFL);

        if (handle < 0 || handle >= MAX_TWAP_ORDERS || !handleActive[handle]) {
            return; // stale timer (handle was freed before it fired) — ignore
        }

        // Idempotency guard: if the cluster replays this log entry, slicesFired will
        // already equal sliceNo + 1, so we skip the dispatch.
        if (slicesFired[handle] != sliceNo) {
            return;
        }

        final int slot = parentSlot[handle];
        final OrderFlyweight parent = orderBook.wrapFlyweight(slot);

        // Compute this slice's quantity (last slice absorbs any rounding remainder)
        final int  remaining    = totalSlices[handle] - slicesFired[handle];
        final long thisSliceQty = (remaining == 1)
                ? parent.leavesQty()               // final slice: send everything left
                : Math.min(sliceQty[handle], parent.leavesQty());

        if (thisSliceQty > 0L && !OrderState.isTerminal(parent.orderState())) {
            dispatchSlice(parent, slot, handle, thisSliceQty);
        }

        slicesFired[handle]++;

        if (slicesFired[handle] >= totalSlices[handle] || parent.leavesQty() <= 0L) {
            // All slices sent — release the handle
            releaseHandle(handle);
        } else {
            // Schedule the next slice
            scheduleNextTimer(handle, timestamp, slicesFired[handle]);
        }
    }

    /** TWAP reacts to child fills only to update parent leavesQty — no new slices here. */
    @Override
    public void onChildFill(final int childSlot, final long lastQty) {
        // Parent leavesQty is updated by OmsClusteredService.handleExecReport()
        // before onChildFill is called. Nothing more needed here.
    }

    @Override
    public void reset() {
        java.util.Arrays.fill(handleActive, false);
        java.util.Arrays.fill(slicesFired, 0);
        freeHandleTop = MAX_TWAP_ORDERS;
        for (int i = 0; i < MAX_TWAP_ORDERS; i++) {
            freeHandles[i] = i;
        }
    }

    // ── Internal helpers ──────────────────────────────────────────────────────

    private void scheduleNextTimer(final int handle, final long nowMs, final int sliceNo) {
        // Encode handle + sliceNo into a single long correlationId
        final long correlationId = ((long) handle << 32) | (sliceNo & 0xFFFFFFFFL);
        final long deadline      = nowMs + (slicesFired[handle] == 0 ? 0L : sliceIntervalMs[handle]);
        cluster.scheduleTimer(correlationId, deadline);
    }

    private void dispatchSlice(
            final OrderFlyweight parent,
            final int parentSlotIdx,
            final int handle,
            final long qty) {

        final int childSlot = orderBook.allocateSlot();
        if (childSlot < 0) {
            return; // book full — in production, alert risk
        }

        final OrderFlyweight child = orderBook.wrapFlyweight(childSlot);
        child.accountId(parent.accountId());
        // Synthesise unique child clOrdId: parent ^ (handle << 32) ^ sliceNo
        child.clOrdId(parent.clOrdId() ^ ((long) handle << 32) ^ slicesFired[handle]);
        child.orderId(OrderLayout.NULL_ID);
        child.origClOrdId(parent.clOrdId());
        child.symbol(parent.symbol());
        child.side(parent.side());
        child.timeInForce(parent.timeInForce());
        child.price(parent.price());
        child.qty(qty);
        child.filledQty(0L);
        child.leavesQty(qty);
        child.orderState(OrderState.PENDING_NEW);
        child.venueId(parent.venueId());

        orderBook.index(childSlot, child.clOrdId(), OrderLayout.NULL_ID);
        sor.routeChildOrder(child, childSlot);
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
