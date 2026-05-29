package com.cobain.oms.model;

/**
 * Binary layout constants for a single Order record stored in a flat, cache-line-friendly buffer.
 *
 * The layout fits in 80 bytes (one and a quarter 64-byte cache lines). All multi-byte
 * fields are naturally aligned so that a single CPU instruction can load them without
 * a cross-cache-line split penalty.
 *
 * Byte map (offsets are from the start of the record, not from start of the backing array):
 *
 *   [  0] accountId    : long  8 bytes — internal account identifier
 *   [  8] clOrdId      : long  8 bytes — client-assigned order ID (FIX tag 11)
 *   [ 16] orderId      : long  8 bytes — venue-assigned order ID (FIX tag 37)
 *   [ 24] origClOrdId  : long  8 bytes — original ClOrdID for cancel/replace (FIX tag 41)
 *   [ 32] symbol       : long  8 bytes — ASCII ticker packed into a long, up to 8 chars
 *   [ 40] price        : long  8 bytes — fixed-point, multiply/divide by PRICE_MULTIPLIER
 *   [ 48] qty          : long  8 bytes — total order quantity
 *   [ 56] filledQty    : long  8 bytes — cumulative filled quantity (FIX tag 14)
 *   [ 64] leavesQty    : long  8 bytes — remaining open quantity  (FIX tag 151)
 *   [ 72] side         : byte  1 byte  — Side constants (see Side.java)
 *   [ 73] timeInForce  : byte  1 byte  — TIF constants (see TimeInForce.java)
 *   [ 74] orderState   : byte  1 byte  — OrderState constants (see OrderState.java)
 *   [ 75] reserved     : byte  1 byte  — future flags (zero-fill)
 *   [ 76] venueId      : int   4 bytes — routing target (maps to an Aeron Publication)
 *   [ 80] — END (total 80 bytes)
 *
 * ZERO-GC CONSTRAINT: This class is loaded once at JVM startup. It contains only static
 * final primitives. No instances are ever created; therefore it adds nothing to GC pressure.
 */
public final class OrderLayout {

    // ── Field offsets ──────────────────────────────────────────────────────────
    public static final int ACCOUNT_ID_OFFSET      = 0;
    public static final int CL_ORD_ID_OFFSET       = 8;
    public static final int ORDER_ID_OFFSET        = 16;
    public static final int ORIG_CL_ORD_ID_OFFSET  = 24;
    public static final int SYMBOL_OFFSET          = 32;
    public static final int PRICE_OFFSET           = 40;
    public static final int QTY_OFFSET             = 48;
    public static final int FILLED_QTY_OFFSET      = 56;
    public static final int LEAVES_QTY_OFFSET      = 64;
    public static final int SIDE_OFFSET            = 72;
    public static final int TIME_IN_FORCE_OFFSET   = 73;
    public static final int ORDER_STATE_OFFSET     = 74;
    public static final int RESERVED_OFFSET        = 75;
    public static final int VENUE_ID_OFFSET        = 76;

    /** Total size of one order record in bytes. */
    public static final int MESSAGE_SIZE = 80;

    /**
     * Fixed-point price multiplier.
     * An actual price of £12.3456 is stored as the long 123456.
     * To recover the display price: storedValue / PRICE_MULTIPLIER.
     * All arithmetic on prices must remain in fixed-point longs to guarantee
     * zero-allocation and exact integer arithmetic with no rounding error.
     */
    public static final long PRICE_MULTIPLIER = 10_000L;

    /**
     * Sentinel used to signal "no order at this slot" or "field not set".
     * We chose Long.MIN_VALUE so it is distinct from every valid 64-bit ID.
     */
    public static final long NULL_ID = Long.MIN_VALUE;

    private OrderLayout() { /* static utility — never instantiate */ }
}