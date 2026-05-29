package com.cobain.oms.core;

import com.cobain.oms.model.OrderEvent;
import com.cobain.oms.model.OrderState;

/**
 * Deterministic sell-side order state machine.
 *
 * ┌─────────────────────────────────────────────────────────────────────────┐
 * │  DETERMINISM GUARANTEE                                                  │
 * │  The entire transition table is a static final primitive array          │
 * │  (byte[NUM_STATES][NUM_EVENTS]) initialised at class-load time.        │
 * │  A transition is a single two-dimensional array lookup + a comparison  │
 * │  — no branching, no object creation, no method dispatch overhead.      │
 * │                                                                         │
 * │  Because the table is static and immutable, every replica in the Aeron │
 * │  Cluster will produce identical state transitions when replaying the    │
 * │  same Raft log entries, guaranteeing deterministic failover.           │
 * └─────────────────────────────────────────────────────────────────────────┘
 *
 * Valid transitions (read: "from → event → to"):
 *
 *  PENDING_NEW  + EXEC_NEW          → NEW
 *  PENDING_NEW  + EXEC_PARTIAL_FILL → PARTIALLY_FILLED
 *  PENDING_NEW  + EXEC_FILL         → FILLED
 *  PENDING_NEW  + EXEC_REJECTED     → REJECTED
 *  PENDING_NEW  + CANCEL_REQUEST    → PENDING_CANCEL   (queue cancel while pending)
 *
 *  NEW          + EXEC_PARTIAL_FILL → PARTIALLY_FILLED
 *  NEW          + EXEC_FILL         → FILLED
 *  NEW          + CANCEL_REQUEST    → PENDING_CANCEL
 *  NEW          + REPLACE_REQUEST   → PENDING_REPLACE
 *  NEW          + EXEC_EXPIRED      → EXPIRED
 *
 *  PARTIALLY_FILLED + EXEC_PARTIAL_FILL → PARTIALLY_FILLED (loop)
 *  PARTIALLY_FILLED + EXEC_FILL         → FILLED
 *  PARTIALLY_FILLED + CANCEL_REQUEST    → PENDING_CANCEL
 *  PARTIALLY_FILLED + REPLACE_REQUEST   → PENDING_REPLACE
 *
 *  PENDING_CANCEL  + EXEC_CANCELED     → CANCELED
 *  PENDING_CANCEL  + EXEC_PARTIAL_FILL → PARTIALLY_FILLED  (fill raced with cancel)
 *  PENDING_CANCEL  + EXEC_FILL         → FILLED
 *
 *  PENDING_REPLACE + EXEC_REPLACED     → REPLACED
 *  PENDING_REPLACE + EXEC_PARTIAL_FILL → PARTIALLY_FILLED  (fill raced with replace)
 *  PENDING_REPLACE + EXEC_FILL         → FILLED
 *  PENDING_REPLACE + EXEC_REJECTED     → REJECTED
 *
 *  REPLACED + EXEC_PARTIAL_FILL → PARTIALLY_FILLED
 *  REPLACED + EXEC_FILL         → FILLED
 *  REPLACED + CANCEL_REQUEST    → PENDING_CANCEL
 *  REPLACED + REPLACE_REQUEST   → PENDING_REPLACE
 *  REPLACED + EXEC_EXPIRED      → EXPIRED
 *
 *  FILLED / CANCELED / REJECTED / EXPIRED — terminal, no outbound transitions.
 */
public final class OrderStateMachine {

    /**
     * The pre-computed transition table.
     * TRANSITION_TABLE[fromState][event] = toState
     * or OrderEvent.INVALID_TRANSITION (-1) if the event is illegal in that state.
     *
     * Indexed as: (byte cast to int) — both dimensions fit in a CPU cache line.
     */
    public static final byte[][] TRANSITION_TABLE =
            new byte[OrderState.NUM_STATES][OrderEvent.NUM_EVENTS];

    static {
        // Pre-fill everything with INVALID so any unset cell is immediately detectable.
        for (final byte[] row : TRANSITION_TABLE) {
            java.util.Arrays.fill(row, OrderEvent.INVALID_TRANSITION);
        }

        // ── From PENDING_NEW ────────────────────────────────────────────────
        TRANSITION_TABLE[OrderState.PENDING_NEW][OrderEvent.EXEC_NEW]          = OrderState.NEW;
        TRANSITION_TABLE[OrderState.PENDING_NEW][OrderEvent.EXEC_PARTIAL_FILL] = OrderState.PARTIALLY_FILLED;
        TRANSITION_TABLE[OrderState.PENDING_NEW][OrderEvent.EXEC_FILL]         = OrderState.FILLED;
        TRANSITION_TABLE[OrderState.PENDING_NEW][OrderEvent.EXEC_REJECTED]     = OrderState.REJECTED;
        TRANSITION_TABLE[OrderState.PENDING_NEW][OrderEvent.CANCEL_REQUEST]    = OrderState.PENDING_CANCEL;

        // ── From NEW ────────────────────────────────────────────────────────
        TRANSITION_TABLE[OrderState.NEW][OrderEvent.EXEC_PARTIAL_FILL] = OrderState.PARTIALLY_FILLED;
        TRANSITION_TABLE[OrderState.NEW][OrderEvent.EXEC_FILL]         = OrderState.FILLED;
        TRANSITION_TABLE[OrderState.NEW][OrderEvent.CANCEL_REQUEST]    = OrderState.PENDING_CANCEL;
        TRANSITION_TABLE[OrderState.NEW][OrderEvent.REPLACE_REQUEST]   = OrderState.PENDING_REPLACE;
        TRANSITION_TABLE[OrderState.NEW][OrderEvent.EXEC_EXPIRED]      = OrderState.EXPIRED;

        // ── From PARTIALLY_FILLED ────────────────────────────────────────────
        TRANSITION_TABLE[OrderState.PARTIALLY_FILLED][OrderEvent.EXEC_PARTIAL_FILL] = OrderState.PARTIALLY_FILLED;
        TRANSITION_TABLE[OrderState.PARTIALLY_FILLED][OrderEvent.EXEC_FILL]         = OrderState.FILLED;
        TRANSITION_TABLE[OrderState.PARTIALLY_FILLED][OrderEvent.CANCEL_REQUEST]    = OrderState.PENDING_CANCEL;
        TRANSITION_TABLE[OrderState.PARTIALLY_FILLED][OrderEvent.REPLACE_REQUEST]   = OrderState.PENDING_REPLACE;

        // ── From PENDING_CANCEL ──────────────────────────────────────────────
        TRANSITION_TABLE[OrderState.PENDING_CANCEL][OrderEvent.EXEC_CANCELED]     = OrderState.CANCELED;
        TRANSITION_TABLE[OrderState.PENDING_CANCEL][OrderEvent.EXEC_PARTIAL_FILL] = OrderState.PARTIALLY_FILLED;
        TRANSITION_TABLE[OrderState.PENDING_CANCEL][OrderEvent.EXEC_FILL]         = OrderState.FILLED;

        // ── From PENDING_REPLACE ─────────────────────────────────────────────
        TRANSITION_TABLE[OrderState.PENDING_REPLACE][OrderEvent.EXEC_REPLACED]     = OrderState.REPLACED;
        TRANSITION_TABLE[OrderState.PENDING_REPLACE][OrderEvent.EXEC_PARTIAL_FILL] = OrderState.PARTIALLY_FILLED;
        TRANSITION_TABLE[OrderState.PENDING_REPLACE][OrderEvent.EXEC_FILL]         = OrderState.FILLED;
        TRANSITION_TABLE[OrderState.PENDING_REPLACE][OrderEvent.EXEC_REJECTED]     = OrderState.REJECTED;

        // ── From REPLACED (mirror of NEW for ongoing life of the replaced order) ──
        TRANSITION_TABLE[OrderState.REPLACED][OrderEvent.EXEC_PARTIAL_FILL] = OrderState.PARTIALLY_FILLED;
        TRANSITION_TABLE[OrderState.REPLACED][OrderEvent.EXEC_FILL]         = OrderState.FILLED;
        TRANSITION_TABLE[OrderState.REPLACED][OrderEvent.CANCEL_REQUEST]    = OrderState.PENDING_CANCEL;
        TRANSITION_TABLE[OrderState.REPLACED][OrderEvent.REPLACE_REQUEST]   = OrderState.PENDING_REPLACE;
        TRANSITION_TABLE[OrderState.REPLACED][OrderEvent.EXEC_EXPIRED]      = OrderState.EXPIRED;

        // Terminal states (FILLED=6, CANCELED=7, REJECTED=8, EXPIRED=9) stay INVALID
        // — no transitions out of terminal states, enforced by the prefilled table.
    }

    /**
     * Returns the next state for the (currentState, event) pair.
     *
     * HOT PATH — single array read, single comparison. No allocation.
     *
     * @param currentState one of the OrderState byte constants
     * @param event        one of the OrderEvent byte constants
     * @return the resulting OrderState byte, or {@link OrderEvent#INVALID_TRANSITION}
     *         if the event is not legal in that state
     */
    public static byte transition(final byte currentState, final byte event) {
        // Cast to int for array indexing — JVM will bounds-check once; JIT will eliminate
        // the check after it proves the values are always in-range.
        return TRANSITION_TABLE[currentState & 0xFF][event & 0xFF];
    }

    /**
     * Validates and applies a state transition directly to an OrderFlyweight.
     * Combines the table lookup with the write-back so call-sites stay readable.
     *
     * @return true if the transition was valid and applied; false if illegal (no state change).
     */
    public static boolean applyTransition(
            final com.cobain.oms.model.OrderFlyweight order,
            final byte event) {

        final byte nextState = transition(order.orderState(), event);
        if (nextState == OrderEvent.INVALID_TRANSITION) {
            return false; // caller decides whether to log/reject
        }
        order.orderState(nextState);
        return true;
    }

    private OrderStateMachine() { /* static utility — no instances */ }
}
