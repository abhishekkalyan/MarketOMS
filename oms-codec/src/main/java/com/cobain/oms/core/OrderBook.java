package com.cobain.oms.core;

import com.cobain.oms.model.OrderFlyweight;
import com.cobain.oms.model.OrderLayout;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.concurrent.UnsafeBuffer;

import java.util.function.IntConsumer;

/**
 * Zero-allocation order store backed by a single pre-allocated byte array.
 *
 * ┌─────────────────────────────────────────────────────────────────────────┐
 * │  MEMORY MODEL                                                           │
 * │                                                                         │
 * │  backingArray[MAX_ORDERS * MESSAGE_SIZE]:                               │
 * │    slot 0  → bytes [0 .. 79]                                            │
 * │    slot 1  → bytes [80 .. 159]                                          │
 * │    ...                                                                   │
 * │    slot N  → bytes [N*80 .. N*80+79]                                    │
 * │                                                                         │
 * │  slotToOrderId[slot] → 0 means the slot is free.                       │
 * │  clOrdIdToSlot maps clOrdId → slot for O(1) lookup.                    │
 * │  orderIdToSlot maps orderId → slot for O(1) ExecReport lookup.         │
 * │                                                                         │
 * │  Free-slot management uses a primitive int[] stack (freeSlots) so that │
 * │  alloc/free are each a single array read/write + one counter increment. │
 * └─────────────────────────────────────────────────────────────────────────┘
 *
 * ZERO-GC CONSTRAINT: The indices (Long2LongHashMap) are the only heap
 * objects. They are allocated at construction and never trigger GC on the
 * hot path because all operations use the primitive put/get overloads.
 */
public final class OrderBook {

    /** Default maximum number of simultaneously open orders. Power-of-2 for potential bitmask ops. */
    public static final int MAX_ORDERS = 65_536;

    /** Sentinel: slot is free / orderId not found. */
    static final long EMPTY = Long.MIN_VALUE;

    private final int maxOrders;

    // ── Backing storage ───────────────────────────────────────────────────────

    private final byte[] backingArray;
    private final UnsafeBuffer orderBuffer;

    // ── Indices — zero-GC Agrona primitive maps ────────────────────────────────

    /** clOrdId (FIX tag 11) → slot index. Used for duplicate detection and cancel/replace. */
    private final Long2LongHashMap clOrdIdToSlot  = new Long2LongHashMap(EMPTY);

    /** orderId (FIX tag 37) → slot index. Used when routing inbound ExecReports. */
    private final Long2LongHashMap orderIdToSlot  = new Long2LongHashMap(EMPTY);

    // ── Free-slot management: a simple O(1) stack ─────────────────────────────

    private final int[] freeSlots;
    private int freeTop;

    // ── Per-slot quick-lookup array (avoids iterating the map to check occupation) ──

    private final long[] slotToOrderId;

    // ── Reusable flyweight: allocated once, re-wrapped on every access ─────────

    /** Single shared flyweight. Valid only within the single-threaded execution loop. */
    private final OrderFlyweight flyweight = new OrderFlyweight();

    /** Separate flyweight for snapshot iteration so nested wrap() calls don't clobber. */
    private final OrderFlyweight snapshotFlyweight = new OrderFlyweight();

    public OrderBook(final int maxOrders) {
        this.maxOrders    = maxOrders;
        this.backingArray = new byte[maxOrders * OrderLayout.MESSAGE_SIZE];
        this.orderBuffer  = new UnsafeBuffer(backingArray);
        this.freeSlots    = new int[maxOrders];
        this.slotToOrderId = new long[maxOrders];
        this.freeTop      = maxOrders;
        for (int i = 0; i < maxOrders; i++) {
            freeSlots[i] = i;
        }
    }

    public OrderBook() {
        this(MAX_ORDERS);
    }

    public int maxOrders() { return maxOrders; }

    // ── Slot allocation / release ─────────────────────────────────────────────

    /**
     * Allocate a free slot.
     * HOT PATH — one array read + one decrement.
     *
     * @return slot index, or -1 if the order book is full.
     */
    public int allocateSlot() {
        if (freeTop == 0) {
            return -1; // full
        }
        return freeSlots[--freeTop];
    }

    /**
     * Release a slot back to the pool.
     * HOT PATH — one array write + one increment.
     */
    public void freeSlot(final int slot) {
        slotToOrderId[slot] = 0L;
        freeSlots[freeTop++] = slot;
    }

    // ── Index maintenance ─────────────────────────────────────────────────────

    /**
     * Register a newly allocated order into both indices.
     * Called immediately after writing order fields into the slot.
     */
    public void index(final int slot, final long clOrdId, final long orderId) {
        slotToOrderId[slot] = orderId;
        clOrdIdToSlot.put(clOrdId, slot);
        if (orderId != OrderLayout.NULL_ID) {
            orderIdToSlot.put(orderId, slot);
        }
    }

    /**
     * Update the orderId index after receiving a venue-assigned orderId.
     * Called when the first EXEC_NEW ExecReport arrives.
     */
    public void indexOrderId(final int slot, final long orderId) {
        slotToOrderId[slot] = orderId;
        orderIdToSlot.put(orderId, slot);
    }

    /**
     * Remove all index entries for a terminal order.
     * The slot is NOT freed here — the caller does that after ensuring the
     * outbound ExecReport to the client has been offered to the Aeron publication.
     */
    public void unindex(final int slot, final long clOrdId, final long orderId) {
        clOrdIdToSlot.remove(clOrdId);
        if (orderId != OrderLayout.NULL_ID) {
            orderIdToSlot.remove(orderId);
        }
        slotToOrderId[slot] = 0L;
    }

    // ── Lookups — O(1) zero-allocation ────────────────────────────────────────

    /**
     * Find the slot holding the given clOrdId.
     * @return slot index, or -1 if not found.
     */
    public int slotByClOrdId(final long clOrdId) {
        final long slot = clOrdIdToSlot.get(clOrdId);
        return slot == EMPTY ? -1 : (int) slot;
    }

    /**
     * Find the slot holding the given venue-assigned orderId.
     * @return slot index, or -1 if not found.
     */
    public int slotByOrderId(final long orderId) {
        final long slot = orderIdToSlot.get(orderId);
        return slot == EMPTY ? -1 : (int) slot;
    }

    /** Returns true if there is an active order with this clOrdId (used for dedup). */
    public boolean containsClOrdId(final long clOrdId) {
        return clOrdIdToSlot.get(clOrdId) != EMPTY;
    }

    // ── Buffer access — for flyweight wrapping and snapshot serialization ──────

    /**
     * Returns the byte offset into orderBuffer where the given slot begins.
     * HOT PATH — single multiply.
     */
    public int offsetForSlot(final int slot) {
        return slot * OrderLayout.MESSAGE_SIZE;
    }

    /**
     * Wraps the shared flyweight at the given slot and returns it.
     * HOT PATH — one multiply, two field assignments.
     * Caller must not hold a reference across the next call to wrapFlyweight().
     */
    public OrderFlyweight wrapFlyweight(final int slot) {
        return flyweight.wrap(orderBuffer, offsetForSlot(slot));
    }

    /** Returns the raw backing UnsafeBuffer — used by SnapshotManager for bulk serialization. */
    public UnsafeBuffer buffer() { return orderBuffer; }

    /** Number of currently open (active) orders — O(1) via the clOrdId map size. */
    public int openOrderCount() { return clOrdIdToSlot.size(); }

    /**
     * Iterates over every occupied slot, passing the slot index to the consumer.
     * Off hot path — used only by SnapshotManager during onTakeSnapshot.
     *
     * Uses Long2LongHashMap.forEach which internally avoids boxing on Agrona ≥ 1.18.
     */
    public void forEachActiveSlot(final IntConsumer consumer) {
        // Long2LongHashMap.forEach resolves to Map.forEach(BiConsumer<Long,Long>) here;
        // the double cast (long) then (int) avoids the "Long cannot be cast to int" error.
        orderIdToSlot.forEach((orderId, slot) -> consumer.accept((int) (long) slot));
    }

    // ── Snapshot restore ──────────────────────────────────────────────────────

    /**
     * Called during snapshot load to clear and reconstruct the book from scratch.
     * Resets the free-slot stack and both indices, then replays serialized order bytes.
     */
    public void reset() {
        clOrdIdToSlot.clear();
        orderIdToSlot.clear();
        freeTop = maxOrders;
        for (int i = 0; i < maxOrders; i++) {
            freeSlots[i] = i;
            slotToOrderId[i] = 0L;
        }
    }

    /**
     * Re-hydrate a single order slot from snapshot bytes.
     * Reads the order fields from the snapshot buffer and rebuilds indices.
     *
     * @param srcBuffer source buffer (snapshot image fragment)
     * @param srcOffset offset within srcBuffer where this order record starts
     */
    public void restoreOrder(final org.agrona.DirectBuffer srcBuffer, final int srcOffset) {
        final int slot = allocateSlot();
        if (slot < 0) {
            throw new IllegalStateException("OrderBook full during snapshot restore");
        }
        // Bulk copy 80 bytes: snapshot fragment → backing array slot
        orderBuffer.putBytes(offsetForSlot(slot), srcBuffer, srcOffset, OrderLayout.MESSAGE_SIZE);

        // Rebuild indices by reading from the just-restored slot
        snapshotFlyweight.wrap(orderBuffer, offsetForSlot(slot));
        index(slot, snapshotFlyweight.clOrdId(), snapshotFlyweight.orderId());
    }
}
