package com.cobain.oms.core;

import com.cobain.oms.codec.ChildOrderIntentFlyweight;
import com.cobain.oms.model.OrderFlyweight;
import com.cobain.oms.model.OrderState;

/**
 * Validates algo-sor routing instructions against live parent order state before
 * oms-core creates any child order.
 *
 * This is the state-model gatekeeper: no child is ever born without a PASS result.
 *
 * ZERO-ALLOCATION: all checks operate on primitive fields. Returns a byte result code —
 * never throws, never allocates.
 *
 * STATE MODEL RULES enforced here:
 *   1. Parent must exist in OrderBook
 *   2. Parent must be in a routable state: NEW, ROUTING, or PARTIALLY_FILLED
 *   3. sliceQty must be > 0
 *   4. sliceQty must be ≤ parent.leavesQty
 *   5. limitPrice must be > 0
 *   6. venueId must be > 0
 *   7. liveChildQty + sliceQty must not exceed parent.leavesQty (over-allocation guard)
 */
public final class ChildOrderIntentValidator {

    // ── Result codes — primitive bytes, zero allocation ──────────────────────
    public static final byte PASS                        =  0;
    public static final byte REJECT_PARENT_NOT_FOUND     = 20;
    public static final byte REJECT_PARENT_NOT_ROUTABLE  = 21;
    public static final byte REJECT_INVALID_SLICE_QTY    = 22;
    public static final byte REJECT_SLICE_EXCEEDS_LEAVES = 23;
    public static final byte REJECT_INVALID_PRICE        = 24;
    public static final byte REJECT_INVALID_VENUE        = 25;
    public static final byte REJECT_QTY_OVERALLOCATION   = 26;

    public ChildOrderIntentValidator() {}

    /**
     * Validate a ChildOrderIntent against the current parent order state.
     *
     * HOT PATH — ZERO ALLOCATIONS.
     *
     * @param intent       the inbound intent from algo-sor
     * @param parent       the parent order flyweight from OrderBook (null if not found)
     * @param liveChildQty sum of leavesQty across all currently live children
     * @return PASS or a REJECT_* code
     */
    public byte validate(
            final ChildOrderIntentFlyweight intent,
            final OrderFlyweight parent,
            final long liveChildQty) {

        // Rule 1: parent must exist
        if (parent == null) {
            return REJECT_PARENT_NOT_FOUND;
        }

        // Rule 2: parent must be in a routable state
        final byte parentState = parent.orderState();
        if (!isRoutableState(parentState)) {
            return REJECT_PARENT_NOT_ROUTABLE;
        }

        // Rule 3: sliceQty > 0
        final long sliceQty = intent.getSliceQty();
        if (sliceQty <= 0L) {
            return REJECT_INVALID_SLICE_QTY;
        }

        // Rule 4: sliceQty ≤ parent.leavesQty
        final long parentLeaves = parent.leavesQty();
        if (sliceQty > parentLeaves) {
            return REJECT_SLICE_EXCEEDS_LEAVES;
        }

        // Rule 5: limitPrice > 0
        if (intent.getLimitPrice() <= 0L) {
            return REJECT_INVALID_PRICE;
        }

        // Rule 6: venueId > 0
        if (intent.getVenueId() <= 0) {
            return REJECT_INVALID_VENUE;
        }

        // Rule 7: over-allocation guard
        if (liveChildQty + sliceQty > parentLeaves) {
            return REJECT_QTY_OVERALLOCATION;
        }

        return PASS;
    }

    /**
     * Returns true if the parent state permits new child orders to be routed.
     * Pure byte comparison — no allocation, no boxing.
     */
    public static boolean isRoutableState(final byte state) {
        return state == OrderState.NEW
            || state == ParentOrderState.ROUTING
            || state == OrderState.PARTIALLY_FILLED;
    }

    /** Off-hot-path description — allocates a String. */
    public static String describe(final byte result) {
        return switch (result) {
            case PASS                        -> "PASS";
            case REJECT_PARENT_NOT_FOUND     -> "PARENT_NOT_FOUND";
            case REJECT_PARENT_NOT_ROUTABLE  -> "PARENT_NOT_ROUTABLE";
            case REJECT_INVALID_SLICE_QTY    -> "INVALID_SLICE_QTY";
            case REJECT_SLICE_EXCEEDS_LEAVES -> "SLICE_EXCEEDS_LEAVES";
            case REJECT_INVALID_PRICE        -> "INVALID_PRICE";
            case REJECT_INVALID_VENUE        -> "INVALID_VENUE";
            case REJECT_QTY_OVERALLOCATION   -> "QTY_OVERALLOCATION";
            default                          -> "UNKNOWN(" + (result & 0xFF) + ")";
        };
    }
}
