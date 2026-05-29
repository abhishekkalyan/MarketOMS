package com.cobain.oms.algo;

import com.cobain.oms.model.OrderFlyweight;
import com.cobain.oms.model.OrderLayout;
import io.aeron.ExclusivePublication;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Smart Order Router — splits a validated parent order across venues according to
 * a mock market-depth array and dispatches child order flyweights via Aeron IPC
 * to the downstream FIX connectivity engines.
 *
 * ┌─────────────────────────────────────────────────────────────────────────┐
 * │  ZERO-GC DESIGN                                                         │
 * │                                                                         │
 * │  • Market depth is a pre-allocated UnsafeBuffer (no List<DepthLevel>).  │
 * │  • Outbound child messages are written into a single pre-allocated      │
 * │    outboundBuffer, then offered to the target venue's ExclusivePublication. │
 * │  • The Aeron offer() is a lock-free SPSC ring-buffer write — no heap   │
 * │    allocation, no OS calls, no system-call boundary crossing.           │
 * │  • All arithmetic (price/qty splitting) is in fixed-point longs.       │
 * └─────────────────────────────────────────────────────────────────────────┘
 *
 * Market depth layout (per level, 24 bytes, up to MAX_DEPTH_LEVELS levels):
 *   [ 0] price     : long  (8 bytes) — fixed-point x10000
 *   [ 8] available : long  (8 bytes) — available qty at this level
 *   [16] venueId   : int   (4 bytes) — maps to venuePublications[venueId]
 *   [20] reserved  : int   (4 bytes)
 *
 * Outbound FIX Binary Format (written to the Aeron publication):
 *   The outboundBuffer is formatted as the OMS-to-FIX binary protocol defined in
 *   FIXMessageEncoder; the SOR writes an OrderLayout.MESSAGE_SIZE block and the
 *   FIX engine on the other side translates it to wire FIX.
 */
public final class SmartOrderRouter {

    // ── Market depth configuration ─────────────────────────────────────────────
    public static final int MAX_DEPTH_LEVELS = 10;
    public static final int DEPTH_LEVEL_SIZE = 24; // bytes per depth level

    private static final int DEPTH_PRICE_OFFSET  = 0;
    private static final int DEPTH_QTY_OFFSET    = 8;
    private static final int DEPTH_VENUE_OFFSET  = 16;

    /** Pre-allocated depth snapshot — updated externally by market data feed. */
    private final UnsafeBuffer marketDepth =
            new UnsafeBuffer(new byte[MAX_DEPTH_LEVELS * DEPTH_LEVEL_SIZE]);

    /** Number of valid levels currently in marketDepth. */
    private int depthLevels = 0;

    // ── Outbound buffer pool ───────────────────────────────────────────────────

    /**
     * Single pre-allocated outbound buffer.
     * Safe because the OMS runs a single-threaded execution loop:
     * we write, offer, and are done before the next message arrives.
     * The Aeron offer() completes synchronously for IPC (shared memory ring).
     */
    private final UnsafeBuffer outboundBuffer =
            new UnsafeBuffer(new byte[OrderLayout.MESSAGE_SIZE]);

    // ── Venue publication array ────────────────────────────────────────────────

    /**
     * One ExclusivePublication per venue ID.
     * Index 0 = default/primary venue.
     * Publications are created at startup and held for the lifetime of the node.
     */
    private final ExclusivePublication[] venuePublications;

    private final int numVenues;

    public SmartOrderRouter(final ExclusivePublication[] venuePublications) {
        this.venuePublications = venuePublications;
        this.numVenues         = venuePublications.length;
    }

    // ── Market depth updates — called by market data handler, off hot path ─────

    /**
     * Refresh the market depth snapshot.
     * @param prices     fixed-point prices at each depth level (ascending for buys)
     * @param quantities available quantity at each level
     * @param venueIds   routing target for each level
     * @param levels     number of valid levels in the arrays
     */
    public void updateDepth(
            final long[] prices,
            final long[] quantities,
            final int[]  venueIds,
            final int    levels) {

        depthLevels = Math.min(levels, MAX_DEPTH_LEVELS);
        for (int i = 0; i < depthLevels; i++) {
            final int base = i * DEPTH_LEVEL_SIZE;
            marketDepth.putLong(base + DEPTH_PRICE_OFFSET, prices[i]);
            marketDepth.putLong(base + DEPTH_QTY_OFFSET,   quantities[i]);
            marketDepth.putInt(base + DEPTH_VENUE_OFFSET,   venueIds[i]);
        }
    }

    // ── Core routing ──────────────────────────────────────────────────────────

    /**
     * Route a child order (or a simple single-venue order) to the appropriate venue.
     *
     * For simple orders: dispatch directly to the preferred venue (child.venueId()).
     * For SOR-split orders: call {@link #routeWithDepthSplit(OrderFlyweight, int)}.
     *
     * HOT PATH — one outboundBuffer write + one Aeron ExclusivePublication.offer().
     */
    public void routeChildOrder(final OrderFlyweight child, final int childSlot) {
        final int venueId = child.venueId();
        if (venueId >= 0 && venueId < numVenues) {
            writeAndOffer(child, venueId);
        } else {
            // VenueId not set — route to best venue according to depth
            writeAndOffer(child, bestVenueForOrder(child));
        }
    }

    /**
     * SOR depth-split: take a parent order, generate child orders that sweep the
     * visible depth levels until the parent quantity is fully allocated.
     *
     * For each depth level (best price first):
     *   - Allocate min(level.available, remaining) to this venue
     *   - Build a child order flyweight in the outboundBuffer
     *   - Offer to that venue's Aeron publication
     *
     * HOT PATH — all arithmetic in longs, all writes to pre-allocated buffers.
     *
     * NOTE: This variant does NOT allocate child slots in the OrderBook — it is used
     * for immediate-IOC/FOK flow where we don't need to track the children.
     * For persistent child tracking (Iceberg, TWAP), use IcebergAlgoEngine/TwapAlgoEngine
     * which allocate child slots before calling routeChildOrder().
     */
    public void routeWithDepthSplit(final OrderFlyweight parent, final int parentSlot) {
        long remaining = parent.qty();
        int  childSeq  = 0;

        for (int i = 0; i < depthLevels && remaining > 0L; i++) {
            final int  base       = i * DEPTH_LEVEL_SIZE;
            final long levelQty   = marketDepth.getLong(base + DEPTH_QTY_OFFSET);
            final int  venueId    = marketDepth.getInt(base + DEPTH_VENUE_OFFSET);
            final long levelPrice = marketDepth.getLong(base + DEPTH_PRICE_OFFSET);

            if (levelQty <= 0L || venueId < 0 || venueId >= numVenues) {
                continue;
            }

            final long childQty = Math.min(levelQty, remaining);

            // Build the outbound child order message in the pre-allocated buffer.
            // We do NOT use the OrderBook here — this is a fire-and-forget dispatch.
            outboundBuffer.putLong(OrderLayout.ACCOUNT_ID_OFFSET,    parent.accountId());
            // Unique child clOrdId: parent ^ (parentSlot << 20) ^ (childSeq << 8)
            outboundBuffer.putLong(OrderLayout.CL_ORD_ID_OFFSET,
                    parent.clOrdId() ^ ((long) parentSlot << 20) ^ ((long) childSeq << 8));
            outboundBuffer.putLong(OrderLayout.ORDER_ID_OFFSET,      OrderLayout.NULL_ID);
            outboundBuffer.putLong(OrderLayout.ORIG_CL_ORD_ID_OFFSET, parent.clOrdId());
            outboundBuffer.putLong(OrderLayout.SYMBOL_OFFSET,        parent.symbol());
            outboundBuffer.putLong(OrderLayout.PRICE_OFFSET,         levelPrice);
            outboundBuffer.putLong(OrderLayout.QTY_OFFSET,           childQty);
            outboundBuffer.putLong(OrderLayout.FILLED_QTY_OFFSET,    0L);
            outboundBuffer.putLong(OrderLayout.LEAVES_QTY_OFFSET,    childQty);
            outboundBuffer.putByte(OrderLayout.SIDE_OFFSET,          parent.side());
            outboundBuffer.putByte(OrderLayout.TIME_IN_FORCE_OFFSET, parent.timeInForce());
            outboundBuffer.putByte(OrderLayout.ORDER_STATE_OFFSET,   com.cobain.oms.model.OrderState.PENDING_NEW);
            outboundBuffer.putInt(OrderLayout.VENUE_ID_OFFSET,       venueId);

            // Offer to venue's Aeron IPC ring — lock-free, sub-microsecond
            offerToVenue(venueId);

            remaining -= childQty;
            childSeq++;
        }
    }

    // ── Internal helpers ──────────────────────────────────────────────────────

    /**
     * Copy flyweight content into outboundBuffer and offer to the target venue.
     * The single copyFrom() is a 80-byte memcpy — the smallest possible copy we can do
     * while still giving the FIX engine its own durable copy of the message.
     */
    private void writeAndOffer(final OrderFlyweight child, final int venueId) {
        // copyFrom does a direct putBytes(80 bytes) — single memcpy
        outboundBuffer.putBytes(0, child.buffer(), child.offset(), OrderLayout.MESSAGE_SIZE);
        offerToVenue(venueId);
    }

    /**
     * Non-blocking offer to the Aeron ExclusivePublication for this venue.
     * ExclusivePublication.offer() is lock-free: it writes to the IPC ring buffer
     * (shared memory) without any OS call or blocking when there is space.
     *
     * Returns negative on back-pressure; caller must decide on retry policy.
     * For the hot path, back-pressure is a risk signal — we don't spin here.
     */
    private long offerToVenue(final int venueId) {
        return venuePublications[venueId].offer(outboundBuffer, 0, OrderLayout.MESSAGE_SIZE);
    }

    /** Select the depth level with the best available price for this order's side. */
    private int bestVenueForOrder(final OrderFlyweight order) {
        // Simplified: return first venue with available qty
        for (int i = 0; i < depthLevels; i++) {
            final long qty = marketDepth.getLong(i * DEPTH_LEVEL_SIZE + DEPTH_QTY_OFFSET);
            if (qty > 0L) {
                return marketDepth.getInt(i * DEPTH_LEVEL_SIZE + DEPTH_VENUE_OFFSET);
            }
        }
        return 0; // default fallback
    }
}
