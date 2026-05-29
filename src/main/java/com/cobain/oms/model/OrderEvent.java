package com.cobain.oms.model;

/**
 * Events that drive the sell-side order state machine.
 *
 * Events arrive from two sources:
 *   1. Inbound client commands  — NEW_ORDER_SINGLE, CANCEL_REQUEST, REPLACE_REQUEST.
 *   2. Inbound venue ExecReports — EXEC_* variants decoded by FIXMessageDecoder.
 *
 * All constants are primitive bytes; the state machine uses them directly as array indices
 * into OrderStateMachine.TRANSITION_TABLE[currentState][event], giving O(1) transition
 * lookup via a single two-dimensional array dereference.
 *
 * INVALID_TRANSITION is the sentinel written into every cell of the table that has no
 * valid transition. The hot path checks only: nextState != INVALID_TRANSITION.
 */
public final class OrderEvent {

    // ── Client-originated commands ─────────────────────────────────────────────
    public static final byte NEW_ORDER_SINGLE   = 0;  // FIX MsgType='D'
    public static final byte CANCEL_REQUEST     = 1;  // FIX MsgType='F'
    public static final byte REPLACE_REQUEST    = 2;  // FIX MsgType='G'

    // ── Venue execution reports (FIX MsgType='8', decoded by ExecType field) ───
    public static final byte EXEC_NEW           = 3;  // ExecType='0' — order accepted
    public static final byte EXEC_PARTIAL_FILL  = 4;  // ExecType='1' — partial execution
    public static final byte EXEC_FILL          = 5;  // ExecType='2' — full execution
    public static final byte EXEC_CANCELED      = 6;  // ExecType='4' — cancel confirmed
    public static final byte EXEC_REPLACED      = 7;  // ExecType='5' — replace confirmed
    public static final byte EXEC_REJECTED      = 8;  // ExecType='8' — new/replace rejected
    public static final byte EXEC_EXPIRED       = 9;  // ExecType='C' — expired (DAY, GTD)

    /** Total event count — used to size the second dimension of the transition table. */
    public static final int NUM_EVENTS = 10;

    /**
     * Sentinel returned by transition lookup when the (state, event) pair is illegal.
     * Stored as -1 (0xFF as unsigned byte) so it cannot alias any valid OrderState.
     */
    public static final byte INVALID_TRANSITION = -1;

    /** Human-readable — off hot path only. */
    public static String nameOf(byte event) {
        return switch (event) {
            case NEW_ORDER_SINGLE  -> "NEW_ORDER_SINGLE";
            case CANCEL_REQUEST    -> "CANCEL_REQUEST";
            case REPLACE_REQUEST   -> "REPLACE_REQUEST";
            case EXEC_NEW          -> "EXEC_NEW";
            case EXEC_PARTIAL_FILL -> "EXEC_PARTIAL_FILL";
            case EXEC_FILL         -> "EXEC_FILL";
            case EXEC_CANCELED     -> "EXEC_CANCELED";
            case EXEC_REPLACED     -> "EXEC_REPLACED";
            case EXEC_REJECTED     -> "EXEC_REJECTED";
            case EXEC_EXPIRED      -> "EXEC_EXPIRED";
            default                -> "UNKNOWN(" + event + ")";
        };
    }

    private OrderEvent() {}
}
