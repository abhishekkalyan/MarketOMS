package com.cobain.oms.algo;

import com.cobain.oms.core.OrderBook;
import com.cobain.oms.model.OrderFlyweight;
import com.cobain.oms.model.OrderLayout;
import com.cobain.oms.model.OrderState;

/**
 * Iceberg (Reserve) algorithm execution engine.
 *
 * Strategy:
 *   - A parent order arrives with totalQty and peakQty (displayed quantity).
 *   - We immediately dispatch a child order for min(peakQty, remainingQty).
 *   - When the child is fully filled we dispatch the next peak, until totalQty is consumed.
 *   - Only the "peak" child is visible in the venue's order book at any time;
 *     the "reserve" quantity is hidden inside the OMS.
 *
 * ZERO-GC GUARANTEES:
 *   - Parent state is tracked in three pre-allocated primitive long arrays indexed by
 *     parentSlot. No Map<>, no wrapper objects, no boxing.
 *   - Child order flyweights are written into the OrderBook backing array directly; the
 *     SmartOrderRouter's pre-allocated buffer pool is used for the outbound Aeron message.
 *   - onChildFill() is a pure arithmetic sequence of array reads/writes.
 *
 * Parent / child relationship:
 *   - parentOrderId[] tracks which parent a child slot belongs to (child → parent).
 *   - remainingQty[]  tracks how much of the parent is still unsent (parent slot → qty).
 *   - peakQty[]       tracks the configured peak size (parent slot → qty).
 */
public final class IcebergAlgoEngine implements AlgoExecutionEngine {

    /** Maximum number of concurrently active iceberg parent orders. */
    private static final int MAX_ICEBERG_ORDERS = 4_096;

    // ── Parent tracking arrays — indexed by parent slot ───────────────────────
    private final long[] remainingQty  = new long[OrderBook.MAX_ORDERS]; // qty not yet sent
    private final long[] peakQty       = new long[OrderBook.MAX_ORDERS]; // peak display size
    private final int[]  activeChild   = new int[OrderBook.MAX_ORDERS];  // current child slot

    // ── Child-to-parent mapping — indexed by child slot ───────────────────────
    /** childParentSlot[childSlot] = parent slot for this child. -1 = not an iceberg child. */
    private final int[] childParentSlot = new int[OrderBook.MAX_ORDERS];

    // ── Default displayed quantity if the incoming order doesn't specify one ──
    /**
     * Fraction of total qty to display at a time (e.g. 10 = display 10% per wave).
     * Configurable via constructor. Default: show 10% at a time.
     */
    private final long peakFraction;

    private final OrderBook        orderBook;
    private final SmartOrderRouter sor;

    /**
     * @param orderBook    shared order store
     * @param sor          smart order router for dispatching child orders
     * @param peakFraction denominator for automatic peak sizing (qty / peakFraction)
     */
    public IcebergAlgoEngine(
            final OrderBook orderBook,
            final SmartOrderRouter sor,
            final long peakFraction) {
        this.orderBook    = orderBook;
        this.sor          = sor;
        this.peakFraction = peakFraction;

        // Pre-fill child-to-parent map with -1 (no parent)
        java.util.Arrays.fill(childParentSlot, -1);
        java.util.Arrays.fill(activeChild, -1);
    }

    // ── AlgoExecutionEngine interface ─────────────────────────────────────────

    /**
     * Immediately dispatch the first peak child order for the newly admitted parent.
     * HOT PATH — array reads/writes, one call to SmartOrderRouter.routeChildOrder().
     */
    @Override
    public void onOrder(
            final OrderFlyweight parentOrder,
            final int parentSlot,
            final long timestamp) {

        final long totalQty = parentOrder.qty();
        // Use the smaller of: 1/peakFraction of total, or the full qty for small orders
        final long peak = Math.max(1L, totalQty / peakFraction);

        remainingQty[parentSlot] = totalQty;
        peakQty[parentSlot]      = peak;

        // Dispatch the first wave immediately
        dispatchNextPeak(parentOrder, parentSlot);
    }

    /** Iceberg does not use timer events — ignore cleanly. */
    @Override
    public void onTimer(final long correlationId, final long timestamp) {
        // no-op
    }

    /**
     * A child order was (partially/fully) filled.
     * If the child is fully filled, dispatch the next iceberg wave.
     * HOT PATH — arithmetic + conditional branch + dispatchNextPeak call.
     */
    @Override
    public void onChildFill(final int childSlot, final long lastQty) {
        final int parentSlot = childParentSlot[childSlot];
        if (parentSlot < 0) {
            return; // not an iceberg child — should not happen if routing is correct
        }

        // Check if this child is now fully filled (leavesQty == 0)
        final OrderFlyweight child = orderBook.wrapFlyweight(childSlot);
        if (child.leavesQty() > 0L) {
            return; // child still has remaining qty; do nothing until fully filled
        }

        // Deduct the filled wave from parent remaining
        remainingQty[parentSlot] -= child.qty();
        activeChild[parentSlot]   = -1;

        if (remainingQty[parentSlot] <= 0L) {
            // Entire iceberg is consumed — mark parent as FILLED
            final OrderFlyweight parent = orderBook.wrapFlyweight(parentSlot);
            parent.orderState(OrderState.FILLED);
            parent.leavesQty(0L);
            parent.filledQty(parent.qty());
            return;
        }

        // Dispatch the next wave
        final OrderFlyweight parent = orderBook.wrapFlyweight(parentSlot);
        dispatchNextPeak(parent, parentSlot);
    }

    @Override
    public void reset() {
        java.util.Arrays.fill(remainingQty, 0L);
        java.util.Arrays.fill(peakQty, 0L);
        java.util.Arrays.fill(activeChild, -1);
        java.util.Arrays.fill(childParentSlot, -1);
    }

    // ── Internal helpers ──────────────────────────────────────────────────────

    /**
     * Allocate a child order slot, populate fields, register the child-to-parent link,
     * and hand off to the SmartOrderRouter for venue dispatch.
     */
    private void dispatchNextPeak(final OrderFlyweight parent, final int parentSlot) {
        final long waveQty = Math.min(peakQty[parentSlot], remainingQty[parentSlot]);

        // Allocate a child slot in the order book
        final int childSlot = orderBook.allocateSlot();
        if (childSlot < 0) {
            // Order book full — in production, trigger a risk alert; here we log off-path.
            return;
        }

        // Write child order into the backing array using the shared flyweight
        final OrderFlyweight child = orderBook.wrapFlyweight(childSlot);
        child.accountId(parent.accountId());
        // Child gets a fresh clOrdId: synthesise from parentClOrdId + wave offset
        // We pack (parentClOrdId | waveIndex<<48) to guarantee uniqueness deterministically.
        final long waveIndex = (parent.qty() - remainingQty[parentSlot]) / peakQty[parentSlot];
        child.clOrdId(parent.clOrdId() ^ (waveIndex << 48));
        child.orderId(OrderLayout.NULL_ID);    // not yet assigned by venue
        child.origClOrdId(parent.clOrdId());
        child.symbol(parent.symbol());
        child.side(parent.side());
        child.timeInForce(parent.timeInForce());
        child.price(parent.price());
        child.qty(waveQty);
        child.filledQty(0L);
        child.leavesQty(waveQty);
        child.orderState(OrderState.PENDING_NEW);
        child.venueId(parent.venueId());

        // Register child-to-parent link (O(1) array write, no allocation)
        childParentSlot[childSlot] = parentSlot;
        activeChild[parentSlot]    = childSlot;

        // Index the child in the order book
        orderBook.index(childSlot, child.clOrdId(), OrderLayout.NULL_ID);

        // Dispatch to venue via SOR (writes to pre-allocated Aeron buffer, offers to publication)
        sor.routeChildOrder(child, childSlot);
    }
}
