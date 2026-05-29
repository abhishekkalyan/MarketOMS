package com.cobain.oms.model;

/**
 * Order-lifecycle state constants encoded as primitive bytes.
 *
 * WHY bytes instead of an enum?
 *   Java enums are objects on the heap. Comparing enum.ordinal() to another ordinal involves
 *   a virtual call + integer comparison. By contrast, a byte comparison is a single CPU
 *   instruction and the constant lives in the constant pool — zero heap allocation,
 *   zero indirection. The transition table in OrderStateMachine is indexed directly by these
 *   byte values (cast to int), giving O(1) array lookup.
 *
 * Terminal states (FILLED, CANCELED, REJECTED, EXPIRED) have no outbound transitions in
 * the OrderStateMachine.TRANSITION_TABLE and therefore no further state changes are possible
 * once reached — enforcing determinism across Raft log replay.
 */
public final class OrderState {

    // ── Live states ────────────────────────────────────────────────────────────
    public static final byte PENDING_NEW      = 0;  // order created, not yet acked by venue
    public static final byte NEW              = 1;  // venue confirmed order is live
    public static final byte PARTIALLY_FILLED = 2;  // one or more partial fills received
    public static final byte PENDING_CANCEL   = 3;  // cancel request sent, awaiting venue ack
    public static final byte PENDING_REPLACE  = 4;  // replace request sent, awaiting venue ack
    public static final byte REPLACED         = 5;  // venue confirmed replace; order is live again

    // ── Terminal states — no further transitions allowed ───────────────────────
    public static final byte FILLED           = 6;
    public static final byte CANCELED         = 7;
    public static final byte REJECTED         = 8;
    public static final byte EXPIRED          = 9;

    /** Total number of distinct states — used to size the transition table. */
    public static final int NUM_STATES = 10;

    /** Returns true if the state is terminal (no further transitions). */
    public static boolean isTerminal(byte state) {
        // Bit-parallel check: terminal states are 6,7,8,9 — faster than four comparisons.
        return state >= FILLED;
    }

    /**
     * Human-readable name — ONLY for logging / diagnostics. Never call on the hot path.
     * Sealed switch expression with Java 21 exhaustive pattern matching.
     */
    public static String nameOf(byte state) {
        return switch (state) {
            case PENDING_NEW      -> "PENDING_NEW";
            case NEW              -> "NEW";
            case PARTIALLY_FILLED -> "PARTIALLY_FILLED";
            case PENDING_CANCEL   -> "PENDING_CANCEL";
            case PENDING_REPLACE  -> "PENDING_REPLACE";
            case REPLACED         -> "REPLACED";
            case FILLED           -> "FILLED";
            case CANCELED         -> "CANCELED";
            case REJECTED         -> "REJECTED";
            case EXPIRED          -> "EXPIRED";
            default               -> "UNKNOWN(" + state + ")";
        };
    }

    private OrderState() {}
}