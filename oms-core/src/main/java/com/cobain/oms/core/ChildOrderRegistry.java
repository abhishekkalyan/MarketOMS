package com.cobain.oms.core;

import com.cobain.oms.codec.ChildOrderIntentFlyweight;
import com.cobain.oms.model.OrderFlyweight;
import com.cobain.oms.model.OrderLayout;
import com.cobain.oms.model.OrderState;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Golden-source store for all child orders.
 *
 * Every child order created by oms-core lives exclusively in this registry.
 * Child orders are linked to their parents via the parent-child fields in
 * OrderLayout (OFFSET_CHILD_COUNT, OFFSET_NEXT_SIBLING_SLOT,
 * OFFSET_PARENT_OR_FIRST_CHILD_ID).
 *
 * ZERO-ALLOCATION on the hot path: all data structures are pre-allocated.
 * Two flyweights (sharedFlyweight, secondFlyweight) prevent clobbering when
 * both a child and parent must be examined in the same work cycle.
 *
 * Linked-list layout:
 *   parent.parentOrFirstChildId() = orderId of first child (0 = no children)
 *   child.nextSiblingSlot()       = slot index of next sibling (-1 = end of list)
 *   child.parentOrFirstChildId()  = orderId of the parent
 */
public final class ChildOrderRegistry {

    public static final int DEFAULT_CAPACITY = 32_768;

    private static final long MISSING    = Long.MIN_VALUE;
    private static final int  NO_SIBLING = -1;

    private final UnsafeBuffer        store;
    private final int                 capacity;
    private final Long2LongHashMap    orderIdToSlot;
    private final Long2LongHashMap    clOrdIdToOrderId;
    private final int[]               freeSlots;
    private int                       freeTop;

    /** Primary flyweight — callers must not retain references past the return. */
    private final OrderFlyweight sharedFlyweight;

    /**
     * Secondary flyweight used when applyFillAndAggregate() must hold
     * both a child and a parent view simultaneously.
     */
    private final OrderFlyweight secondFlyweight;

    @FunctionalInterface
    public interface CancelRequestSink {
        void onCancelRequest(OrderFlyweight childInPendingCancelState);
    }

    public ChildOrderRegistry(final int capacity) {
        this.capacity         = capacity;
        this.store            = new UnsafeBuffer(new byte[capacity * OrderLayout.BLOCK_LENGTH]);
        this.orderIdToSlot    = new Long2LongHashMap(MISSING);
        this.clOrdIdToOrderId = new Long2LongHashMap(MISSING);
        this.freeSlots        = new int[capacity];
        this.freeTop          = capacity;
        for (int i = 0; i < capacity; i++) {
            freeSlots[i] = i;
        }
        this.sharedFlyweight = new OrderFlyweight();
        this.secondFlyweight = new OrderFlyweight();
    }

    public ChildOrderRegistry() {
        this(DEFAULT_CAPACITY);
    }

    // ── Core operations ───────────────────────────────────────────────────────

    /**
     * Create a child order from a validated ChildOrderIntent.
     * Links the child into the parent's linked list (prepend, O(1)).
     *
     * Returns the child flyweight (sharedFlyweight) pointing at the new slot.
     * Returns null if the registry is full (capacity exhausted — ops alert condition).
     *
     * HOT PATH — ZERO ALLOCATIONS.
     */
    public OrderFlyweight createChild(
            final ChildOrderIntentFlyweight intent,
            final OrderFlyweight parent,
            final long childOrderId) {

        if (freeTop == 0) {
            return null;
        }
        final int slot = freeSlots[--freeTop];

        sharedFlyweight.wrap(store, slot * OrderLayout.BLOCK_LENGTH);

        // Child clOrdId: parentOrderId * 10_000 + sliceIndex
        // Globally unique, self-describing, FIX-safe (fits in a long)
        final long childClOrdId = intent.getParentOrderId() * 10_000L
                                + (intent.getSliceIndex() & 0xFF);

        // Populate child fields from intent + parent
        sharedFlyweight.accountId(parent.accountId());
        sharedFlyweight.clOrdId(childClOrdId);
        sharedFlyweight.orderId(childOrderId);
        sharedFlyweight.origClOrdId(parent.clOrdId());
        sharedFlyweight.symbol(parent.symbol());
        sharedFlyweight.side(parent.side());
        sharedFlyweight.timeInForce(parent.timeInForce());
        sharedFlyweight.price(intent.getLimitPrice());
        sharedFlyweight.qty(intent.getSliceQty());
        sharedFlyweight.filledQty(0L);
        sharedFlyweight.leavesQty(intent.getSliceQty());
        sharedFlyweight.orderState(OrderState.PENDING_NEW);
        sharedFlyweight.venueId(intent.getVenueId());
        sharedFlyweight.transactTime(intent.getIntentTimestampNanos());
        sharedFlyweight.childCount(0); // child orders have no children
        sharedFlyweight.parentOrFirstChildId(parent.orderId()); // child → parent link

        // Prepend this child to the parent's linked list
        final long oldFirstChildId = parent.parentOrFirstChildId();
        if (oldFirstChildId != 0L) {
            final long oldHeadSlotL = orderIdToSlot.get(oldFirstChildId);
            sharedFlyweight.nextSiblingSlot(
                oldHeadSlotL != MISSING ? (int) oldHeadSlotL : NO_SIBLING);
        } else {
            sharedFlyweight.nextSiblingSlot(NO_SIBLING);
        }
        parent.parentOrFirstChildId(childOrderId); // new list head
        parent.childCount(parent.childCount() + 1);

        // Index the new child
        orderIdToSlot.put(childOrderId, slot);
        clOrdIdToOrderId.put(childClOrdId, childOrderId);

        return sharedFlyweight;
    }

    /**
     * Apply a fill to a child order and aggregate the quantity delta into the parent.
     *
     * Steps (all zero-allocation):
     *   1. Locate child by childClOrdId
     *   2. Apply fill to child
     *   3. If child.leavesQty == 0: mark child FILLED, decrement parent.childCount
     *   4. Locate parent via parentBook
     *   5. Apply fill to parent
     *   6. Update parent state: FILLED if leavesQty == 0, else PARTIALLY_FILLED
     *   7. Return the parent flyweight (secondFlyweight) for exec-report publication
     *
     * Returns null if the childClOrdId is not found (log and discard).
     *
     * HOT PATH — ZERO ALLOCATIONS.
     */
    public OrderFlyweight applyFillAndAggregate(
            final long childClOrdId,
            final long fillQty,
            final long fillPrice,
            final OrderBook parentBook) {

        // Step 1: locate child
        final long childOrderId = clOrdIdToOrderId.get(childClOrdId);
        if (childOrderId == MISSING) {
            return null;
        }
        final long childSlotL = orderIdToSlot.get(childOrderId);
        if (childSlotL == MISSING) {
            return null;
        }
        final int childSlot = (int) childSlotL;

        // Step 2: apply fill to child (sharedFlyweight = child)
        sharedFlyweight.wrap(store, childSlot * OrderLayout.BLOCK_LENGTH);
        sharedFlyweight.applyFill(fillQty);
        sharedFlyweight.transactTime(System.nanoTime());

        // Step 3: if child fully filled, mark terminal and decrement parent's count
        final boolean childFilled = sharedFlyweight.leavesQty() == 0L;
        if (childFilled) {
            sharedFlyweight.orderState(OrderState.FILLED);
        }

        // Step 4: locate parent (secondFlyweight = parent)
        final long parentOrderId = sharedFlyweight.parentOrFirstChildId();
        final int parentSlot = parentBook.slotByOrderId(parentOrderId);
        if (parentSlot < 0) {
            return null; // orphaned child — should not happen
        }
        secondFlyweight.wrap(parentBook.buffer(), parentBook.offsetForSlot(parentSlot));

        // Step 5: apply fill delta to parent
        secondFlyweight.applyFill(fillQty);
        secondFlyweight.transactTime(System.nanoTime());

        // Decrement live child count if child finished
        if (childFilled) {
            final int liveCount = secondFlyweight.childCount() - 1;
            secondFlyweight.childCount(Math.max(0, liveCount));
        }

        // Step 6: update parent state
        if (secondFlyweight.leavesQty() == 0L) {
            secondFlyweight.orderState(OrderState.FILLED);
        } else if (secondFlyweight.filledQty() > 0L) {
            secondFlyweight.orderState(OrderState.PARTIALLY_FILLED);
        }

        // Step 9: return parent flyweight for exec-report publication
        return secondFlyweight;
    }

    /**
     * Transition all live children of a parent to PENDING_CANCEL and notify the sink.
     * Walks the parent's child linked list in O(n) where n = live child count.
     *
     * Returns count of children given cancel requests.
     * HOT PATH — ZERO ALLOCATIONS.
     */
    public int cancelAllChildren(final OrderFlyweight parent, final CancelRequestSink cancelSink) {
        int count = 0;
        long firstChildId = parent.parentOrFirstChildId();
        if (firstChildId == 0L) {
            return 0;
        }

        // First hop: orderId → slot lookup
        long slotL = orderIdToSlot.get(firstChildId);
        if (slotL == MISSING) {
            return 0;
        }
        int slot = (int) slotL;

        while (slot != NO_SIBLING) {
            sharedFlyweight.wrap(store, slot * OrderLayout.BLOCK_LENGTH);
            final byte state = sharedFlyweight.orderState();
            if (!OrderState.isTerminal(state)) {
                sharedFlyweight.orderState(OrderState.PENDING_CANCEL);
                cancelSink.onCancelRequest(sharedFlyweight);
                count++;
            }
            slot = sharedFlyweight.nextSiblingSlot();
        }
        return count;
    }

    /**
     * Compute the total leavesQty across all live (non-terminal) children of a parent.
     * Used by ChildOrderIntentValidator for the over-allocation guard.
     * O(n) where n = live child count (typically ≤ 20).
     * HOT PATH — ZERO ALLOCATIONS.
     */
    public long computeLiveChildQty(final OrderFlyweight parent) {
        long total = 0L;
        final long firstChildId = parent.parentOrFirstChildId();
        if (firstChildId == 0L) {
            return 0L;
        }
        final long slotL = orderIdToSlot.get(firstChildId);
        if (slotL == MISSING) {
            return 0L;
        }

        int slot = (int) slotL;
        while (slot != NO_SIBLING) {
            sharedFlyweight.wrap(store, slot * OrderLayout.BLOCK_LENGTH);
            if (!OrderState.isTerminal(sharedFlyweight.orderState())) {
                total += sharedFlyweight.leavesQty();
            }
            slot = sharedFlyweight.nextSiblingSlot();
        }
        return total;
    }

    /**
     * Look up a child order by its oms-core-assigned orderId.
     * Returns sharedFlyweight or null if not found.
     * HOT PATH — ZERO ALLOCATIONS.
     */
    public OrderFlyweight getByOrderId(final long childOrderId) {
        final long slotL = orderIdToSlot.get(childOrderId);
        if (slotL == MISSING) {
            return null;
        }
        sharedFlyweight.wrap(store, (int) slotL * OrderLayout.BLOCK_LENGTH);
        return sharedFlyweight;
    }

    /**
     * Look up a child order by its client-assigned clOrdId.
     * Returns sharedFlyweight or null if not found.
     * HOT PATH — ZERO ALLOCATIONS.
     */
    public OrderFlyweight getByClOrdId(final long childClOrdId) {
        final long childOrderId = clOrdIdToOrderId.get(childClOrdId);
        if (childOrderId == MISSING) {
            return null;
        }
        return getByOrderId(childOrderId);
    }

    /**
     * Remove a terminal child from the indexes and return its slot to the free list.
     * Called after the child reaches a terminal state (FILLED, CANCELED, REJECTED).
     * HOT PATH — ZERO ALLOCATIONS.
     */
    public void removeTerminal(final long childOrderId) {
        final long slotL = orderIdToSlot.remove(childOrderId);
        if (slotL == MISSING) {
            return;
        }
        final int slot = (int) slotL;
        sharedFlyweight.wrap(store, slot * OrderLayout.BLOCK_LENGTH);
        final long childClOrdId = sharedFlyweight.clOrdId();
        clOrdIdToOrderId.remove(childClOrdId);
        freeSlots[freeTop++] = slot;
    }

    /** Number of currently registered child orders. */
    public int size() {
        return capacity - freeTop;
    }

    // ── Snapshot support ──────────────────────────────────────────────────────

    /**
     * Serialize all child records to a buffer for Aeron Cluster snapshot.
     * Returns the number of bytes written.
     * Off hot path — allocates nothing but iterates all active records.
     */
    public int snapshot(final MutableDirectBuffer dest, int destOffset) {
        final int countOffset = destOffset;
        destOffset += Integer.BYTES; // reserve space for count

        int count = 0;
        for (int i = 0; i < capacity; i++) {
            // check if slot is occupied: orderId != 0 means occupied
            sharedFlyweight.wrap(store, i * OrderLayout.BLOCK_LENGTH);
            if (sharedFlyweight.orderId() != 0L) {
                dest.putBytes(destOffset, store, i * OrderLayout.BLOCK_LENGTH, OrderLayout.BLOCK_LENGTH);
                destOffset += OrderLayout.BLOCK_LENGTH;
                count++;
            }
        }
        dest.putInt(countOffset, count);
        return destOffset - countOffset;
    }

    /**
     * Restore child records from a snapshot buffer.
     * Rebuilds all indexes and re-links children to their parents in the parentBook.
     * Returns the number of bytes consumed.
     * Off hot path.
     */
    public int restore(final DirectBuffer src, int srcOffset, final OrderBook parentBook) {
        // Clear existing state
        orderIdToSlot.clear();
        clOrdIdToOrderId.clear();
        freeTop = capacity;
        for (int i = 0; i < capacity; i++) {
            freeSlots[i] = i;
        }

        final int count = src.getInt(srcOffset);
        srcOffset += Integer.BYTES;

        for (int i = 0; i < count; i++) {
            if (freeTop == 0) {
                throw new IllegalStateException("ChildOrderRegistry full during snapshot restore");
            }
            final int slot = freeSlots[--freeTop];
            store.putBytes(slot * OrderLayout.BLOCK_LENGTH, src, srcOffset, OrderLayout.BLOCK_LENGTH);
            srcOffset += OrderLayout.BLOCK_LENGTH;

            sharedFlyweight.wrap(store, slot * OrderLayout.BLOCK_LENGTH);
            final long childOrderId = sharedFlyweight.orderId();
            final long childClOrdId = sharedFlyweight.clOrdId();
            orderIdToSlot.put(childOrderId, slot);
            clOrdIdToOrderId.put(childClOrdId, childOrderId);
        }

        return Integer.BYTES + count * OrderLayout.BLOCK_LENGTH;
    }
}
