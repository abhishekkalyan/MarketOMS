package com.cobain.oms.algo;

import com.cobain.oms.codec.ChildOrderIntentFlyweight;
import com.cobain.oms.model.OrderFlyweight;

/**
 * Smart Order Router — splits a validated parent order across venues according to
 * a market-depth snapshot and produces {@link ChildOrderIntentFlyweight} records
 * via the {@link AlgoExecutionEngine.ChildIntentSink} callback.
 *
 * In the intent-driven architecture, the SOR is a PURE COMPUTATION ENGINE.
 * It has NO Aeron dependencies. It dispatches NOTHING to the FIX bridge.
 * Every routing instruction it produces is returned to oms-core as a
 * ChildOrderIntent which oms-core validates, persists, and dispatches.
 *
 * ZERO-GC DESIGN:
 *   • Market depth is passed in as caller-owned primitive arrays (no List<DepthLevel>).
 *   • The pre-allocated ChildOrderIntentFlyweight is reused for every child slice.
 *   • All arithmetic (price/qty splitting) is in fixed-point longs.
 */
public final class SmartOrderRouter {

    public static final int MAX_VENUES = 10;

    private final int maxVenues;

    // ── Pre-allocated intent — reused per route() call ────────────────────────
    private final ChildOrderIntentFlyweight intent;

    public SmartOrderRouter(final int maxVenues) {
        this.maxVenues = maxVenues;
        this.intent    = ChildOrderIntentFlyweight.allocate();
    }

    public SmartOrderRouter() {
        this(MAX_VENUES);
    }

    public int maxVenues() { return maxVenues; }

    // ── Core routing ──────────────────────────────────────────────────────────

    /**
     * Route a parent order across venues using the provided market-depth snapshot.
     * For each venue level with available qty, dispatch a child intent sized to
     * min(level.available, remaining) until the parent qty is fully allocated.
     *
     * @param parent     validated parent order flyweight
     * @param venueIds   venue IDs at each depth level
     * @param venuePrices limit prices at each depth level (fixed-point)
     * @param venueQtys  available quantities at each depth level
     * @param venueCount number of valid entries in the above arrays
     * @param sink       receives one intent per child slice
     * @return number of intents published
     *
     * HOT PATH — all arithmetic in longs, no allocation.
     */
    public int route(
            final OrderFlyweight parent,
            final int[]  venueIds,
            final long[] venuePrices,
            final long[] venueQtys,
            final int    venueCount,
            final AlgoExecutionEngine.ChildIntentSink sink) {

        long remaining = parent.leavesQty();
        int  dispatched = 0;
        byte sliceIndex = 0;
        final long nowNanos = System.nanoTime();

        for (int i = 0; i < venueCount && remaining > 0L; i++) {
            final int  venueId    = venueIds[i];
            final long levelQty   = venueQtys[i];
            final long levelPrice = venuePrices[i];

            if (levelQty <= 0L || venueId <= 0) {
                continue;
            }

            final long childQty = Math.min(levelQty, remaining);

            intent.setParentOrderId(parent.orderId());
            intent.setParentClOrdId(parent.clOrdId());
            intent.setSliceQty(childQty);
            intent.setLimitPrice(levelPrice > 0L ? levelPrice : parent.price());
            intent.setVenueId(venueId);
            intent.setAlgoType(ChildOrderIntentFlyweight.ALGO_SOR);
            intent.setSliceIndex(sliceIndex);
            intent.setIntentTimestampNanos(nowNanos);

            sink.onIntent(intent);
            dispatched++;

            remaining -= childQty;
            sliceIndex = (byte) ((sliceIndex + 1) & 0xFF);
        }

        // If no venue matched (e.g. empty depth), route entire qty to default venue
        if (dispatched == 0 && remaining > 0L) {
            intent.setParentOrderId(parent.orderId());
            intent.setParentClOrdId(parent.clOrdId());
            intent.setSliceQty(remaining);
            intent.setLimitPrice(parent.price());
            intent.setVenueId(1); // default venue
            intent.setAlgoType(ChildOrderIntentFlyweight.ALGO_SOR);
            intent.setSliceIndex((byte) 0);
            intent.setIntentTimestampNanos(nowNanos);

            sink.onIntent(intent);
            dispatched = 1;
        }

        return dispatched;
    }
}
