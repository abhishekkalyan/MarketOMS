package com.cobain.oms.core;

import com.cobain.oms.model.OrderEvent;
import com.cobain.oms.model.OrderState;

/**
 * Parent-order-only state constants that extend the base {@link OrderState} values.
 *
 * ZERO-ALLOCATION: plain byte constants. No enum. No boxing.
 *
 * These constants EXTEND OrderState and must not conflict with base state values 0–9.
 * Parent-only states begin at 10, leaving a gap for future base state additions.
 *
 * ROUTING state lifecycle:
 *   NEW → ROUTING           : first ChildOrderIntent accepted by oms-core
 *   ROUTING → PARTIALLY_FILLED : first child fill aggregated into parent
 *   ROUTING → CANCELED      : all children cancelled before any fill
 *   ROUTING → FILLED        : all qty filled via children (e.g. one large child)
 */
public final class ParentOrderState {

    private ParentOrderState() {}

    /**
     * Parent order has been accepted and one or more child orders have been
     * created and dispatched to venues. Awaiting fills or cancels.
     *
     * Valid incoming events in ROUTING state:
     *   EXEC_PARTIAL_FILL → PARTIALLY_FILLED  (first venue fill arrives)
     *   EXEC_FILL         → FILLED            (full qty filled via children)
     *   CANCEL_REQUEST    → PENDING_CANCEL    (client cancel request)
     *   EXEC_REJECTED     → REJECTED          (venue mass-cancel / bust)
     */
    public static final byte ROUTING = 10;

    /**
     * Register ROUTING transitions into {@link OrderStateMachine#TRANSITION_TABLE}.
     *
     * MUST be called once at startup, before any orders are processed.
     * ZERO-ALLOCATION: mutates the existing static transition table array.
     */
    public static void registerTransitions() {
        OrderStateMachine.TRANSITION_TABLE[ROUTING & 0xFF][OrderEvent.EXEC_PARTIAL_FILL] =
                OrderState.PARTIALLY_FILLED;
        OrderStateMachine.TRANSITION_TABLE[ROUTING & 0xFF][OrderEvent.EXEC_FILL] =
                OrderState.FILLED;
        OrderStateMachine.TRANSITION_TABLE[ROUTING & 0xFF][OrderEvent.CANCEL_REQUEST] =
                OrderState.PENDING_CANCEL;
        OrderStateMachine.TRANSITION_TABLE[ROUTING & 0xFF][OrderEvent.EXEC_REJECTED] =
                OrderState.REJECTED;
    }

    /** Returns true if this state is the ROUTING parent-specific state. */
    public static boolean isRouting(final byte state) {
        return state == ROUTING;
    }
}
