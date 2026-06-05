package com.cobain.oms.algo;

import com.cobain.oms.codec.ChildOrderIntentFlyweight;
import com.cobain.oms.model.OrderFlyweight;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Iceberg (Reserve) algorithm execution engine.
 *
 * Strategy:
 *   - A parent order arrives with totalQty and peakQty (displayed quantity).
 *   - Dispatch one intent per peak until totalQty is consumed.
 *   - Only the "peak" child is visible at any venue at any time.
 *
 * In the intent-driven architecture this engine is a PURE COMPUTATION ENGINE.
 * It creates NO child order state. It has NO OrderBook dependency.
 * All child creation happens inside oms-core after ChildOrderIntentValidator passes.
 *
 * ZERO-GC GUARANTEES:
 *   - Parent remaining/peak qty tracked in Agrona primitive Long2LongHashMaps by parentOrderId.
 *   - The pre-allocated ChildOrderIntentFlyweight is reused for every slice.
 *   - onSlice() dispatches all remaining peaks in one call and returns the count.
 */
public final class IcebergAlgoEngine implements AlgoExecutionEngine {

    private static final Logger log     = LoggerFactory.getLogger(IcebergAlgoEngine.class);
    private static final long   MISSING = Long.MIN_VALUE;
    /** clOrdId formula: parentOrderId * 10_000L + (sliceIndex & 0xFF) → max 256 unique children per parent. */
    static final int MAX_SLICES_PER_PARENT = 256;

    /** Denominator for automatic peak sizing: display 1/peakFraction of total qty. */
    private final long peakFraction;

    // ── Per-parent state — keyed by parentOrderId (not slot) ─────────────────
    private final Long2LongHashMap remainingQty; // parentOrderId → qty not yet sent
    private final Long2LongHashMap peakQty;      // parentOrderId → peak display size

    // ── Pre-allocated intent buffer — reused every slice ─────────────────────
    private final ChildOrderIntentFlyweight intent;

    /**
     * @param peakFraction denominator for automatic peak sizing (qty / peakFraction)
     * @param maxConcurrent maximum number of concurrently tracked iceberg orders
     */
    public IcebergAlgoEngine(final long peakFraction, final int maxConcurrent) {
        this.peakFraction = peakFraction;
        this.remainingQty = new Long2LongHashMap(maxConcurrent * 2, 0.65f, MISSING);
        this.peakQty      = new Long2LongHashMap(maxConcurrent * 2, 0.65f, MISSING);
        this.intent       = ChildOrderIntentFlyweight.allocate();
    }

    public IcebergAlgoEngine(final long peakFraction) {
        this(peakFraction, 4_096);
    }

    // ── AlgoExecutionEngine interface ─────────────────────────────────────────

    /**
     * Dispatch all iceberg peaks for the parent order.
     * Each call to sink.onIntent() represents one child wave.
     * HOT PATH — array reads/writes, no allocation.
     */
    @Override
    public int onSlice(final OrderFlyweight parent, final ChildIntentSink sink, final long nowNanos) {
        final long parentOrderId = parent.orderId();
        final long totalQty      = parent.qty();
        final long peak          = Math.max(1L, totalQty / peakFraction);

        // Register state (idempotent: if already registered from a prior call, update)
        long remaining = remainingQty.get(parentOrderId);
        if (remaining == MISSING) {
            remaining = totalQty;
            remainingQty.put(parentOrderId, remaining);
            peakQty.put(parentOrderId, peak);
        }

        int dispatched = 0;
        byte sliceIndex = 0;
        while (remaining > 0L) {
            if (dispatched >= MAX_SLICES_PER_PARENT) {
                log.warn("Iceberg slice limit reached (256) for orderId={}: {} qty remaining dropped",
                         parentOrderId, remaining);
                break;
            }
            final long waveQty = Math.min(peak, remaining);

            intent.setParentOrderId(parentOrderId);
            intent.setParentClOrdId(parent.clOrdId());
            intent.setSliceQty(waveQty);
            intent.setLimitPrice(parent.price());
            intent.setVenueId(parent.venueId() > 0 ? parent.venueId() : 1);
            intent.setAlgoType(ChildOrderIntentFlyweight.ALGO_ICEBERG);
            intent.setSliceIndex(sliceIndex);
            intent.setIntentTimestampNanos(nowNanos);

            sink.onIntent(intent);
            dispatched++;

            remaining -= waveQty;
            sliceIndex = (byte) ((sliceIndex + 1) & 0xFF);
        }

        remainingQty.put(parentOrderId, 0L);
        return dispatched;
    }

    /** Iceberg does not use timer events. */
    @Override
    public void onTimer(final long correlationId, final long timestamp) {
        // no-op
    }

    @Override
    public void reset() {
        remainingQty.clear();
        peakQty.clear();
    }
}
