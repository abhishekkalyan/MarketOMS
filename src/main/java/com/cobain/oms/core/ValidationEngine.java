package com.cobain.oms.core;

import com.cobain.oms.model.OrderFlyweight;
import com.cobain.oms.model.OrderLayout;
import com.cobain.oms.model.Side;
import com.cobain.oms.model.TimeInForce;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.collections.LongHashSet;

/**
 * Fast-fail inbound validation engine.
 *
 * ALL validation checks return primitive int error codes (defined as constants below).
 * No Exception objects, no String allocations, no boxing — every check on the hot path
 * is a comparison of two longs/bytes or an Agrona primitive map lookup.
 *
 * Validation order is intentionally cheapest-first (eliminating the most traffic with
 * the least work):
 *
 *   1. Duplicate ClOrdID check     — one Long2LongHashMap.get()
 *   2. Side validity               — one byte comparison
 *   3. TIF validity                — one byte comparison
 *   4. Qty > 0                     — one long comparison
 *   5. Price > 0                   — one long comparison
 *   6. Symbol whitelist            — one LongHashSet.contains()
 *   7. Notional limit              — one division + one long comparison
 *
 * De-duplication:
 *   The seenClOrdIds map records clOrdId → orderId for every order accepted since
 *   startup. On failover, Aeron Cluster replays the Raft log so the map is rebuilt
 *   deterministically; the snapshot also captures the map for fast-path recovery.
 *
 * Notional limit math (no overflow, no floating point):
 *   price is stored as fixed-point longs (multiplied by PRICE_MULTIPLIER = 10,000).
 *   Notional = price * qty / PRICE_MULTIPLIER (actual currency value).
 *   To avoid computing price*qty (which can overflow for large orders), we rearrange:
 *     price * qty > maxNotional * PRICE_MULTIPLIER
 *   ⟺  price > (maxNotional * PRICE_MULTIPLIER) / qty    (safe if qty > 0)
 *   Since maxNotional ≤ 10^12 and PRICE_MULTIPLIER = 10^4, their product ≤ 10^16,
 *   which comfortably fits in a long (max ~9.2 × 10^18).
 */
public final class ValidationEngine {

    // ── Result codes — returned as primitive int, zero allocation ─────────────
    public static final int VALID                      = 0;
    public static final int ERR_DUPLICATE_CL_ORD_ID   = 1;
    public static final int ERR_INVALID_SIDE           = 2;
    public static final int ERR_INVALID_TIF            = 3;
    public static final int ERR_INVALID_QTY            = 4;
    public static final int ERR_INVALID_PRICE          = 5;
    public static final int ERR_SYMBOL_NOT_WHITELISTED = 6;
    public static final int ERR_NOTIONAL_LIMIT         = 7;
    public static final int ERR_INVALID_ACCOUNT        = 8;

    // ── State — allocated once at construction, never on the hot path ──────────

    /**
     * De-duplication index: clOrdId → orderId.
     * Populated by registerAccepted(); queried by validate().
     * Persisted in the Aeron Cluster snapshot so failover does not open a duplicate window.
     */
    private final Long2LongHashMap seenClOrdIds;

    /**
     * Permitted symbols encoded as longs (via OrderFlyweight.encodeSymbol).
     * Loaded at startup from configuration; immutable during the trading session.
     */
    private final LongHashSet permittedSymbols;

    /**
     * Maximum notional value in base currency units (not fixed-point).
     * e.g. 10_000_000L = £10 million per order.
     */
    private final long maxNotional;

    private static final long DEDUP_MISSING = Long.MIN_VALUE;

    public ValidationEngine(final long maxNotional, final long... permittedSymbolLongs) {
        this.maxNotional      = maxNotional;
        this.seenClOrdIds     = new Long2LongHashMap(DEDUP_MISSING);
        // LongHashSet(int proposedCapacity) — simplest stable constructor across Agrona versions.
        this.permittedSymbols = new LongHashSet(Math.max(16, permittedSymbolLongs.length * 2));
        for (final long sym : permittedSymbolLongs) {
            permittedSymbols.add(sym);
        }
    }

    // ── Hot-path validation ────────────────────────────────────────────────────

    /**
     * Validate a NewOrderSingle flyweight before it is admitted to the state machine.
     *
     * HOT PATH — all checks are primitive comparisons or single-hop hash lookups.
     * Returns VALID (0) on success; a non-zero ERR_* code identifies the first failure.
     */
    public int validateNewOrder(final OrderFlyweight order) {

        // ── 1. Duplicate ClOrdID ────────────────────────────────────────────
        // seenClOrdIds.get() returns DEDUP_MISSING if the key is absent.
        // We compare against the sentinel to avoid a second lookup via containsKey().
        if (seenClOrdIds.get(order.clOrdId()) != DEDUP_MISSING) {
            return ERR_DUPLICATE_CL_ORD_ID;
        }

        // ── 2. Side ─────────────────────────────────────────────────────────
        if (!Side.isValid(order.side())) {
            return ERR_INVALID_SIDE;
        }

        // ── 3. TimeInForce ───────────────────────────────────────────────────
        if (!TimeInForce.isValid(order.timeInForce())) {
            return ERR_INVALID_TIF;
        }

        // ── 4. Quantity ──────────────────────────────────────────────────────
        if (order.qty() <= 0L) {
            return ERR_INVALID_QTY;
        }

        // ── 5. Price (for limit orders; market orders use price = 0) ─────────
        // We allow price == 0 for market orders (distinguish by TIF or OrdType).
        if (order.price() < 0L) {
            return ERR_INVALID_PRICE;
        }

        // ── 6. Symbol whitelist ──────────────────────────────────────────────
        if (!permittedSymbols.contains(order.symbol())) {
            return ERR_SYMBOL_NOT_WHITELISTED;
        }

        // ── 7. Notional limit ────────────────────────────────────────────────
        // Overflow-safe check: price > (maxNotional * PRICE_MULTIPLIER) / qty
        // This is equivalent to price * qty > maxNotional * PRICE_MULTIPLIER
        // but avoids 128-bit overflow entirely by using integer division.
        if (order.price() > 0L) {
            final long maxNotionalFixed = maxNotional * OrderLayout.PRICE_MULTIPLIER;
            // maxNotionalFixed / qty gives the maximum allowable per-unit price
            if (order.price() > maxNotionalFixed / order.qty()) {
                return ERR_NOTIONAL_LIMIT;
            }
        }

        return VALID;
    }

    /**
     * Validate a CancelRequest or CancelReplaceRequest flyweight.
     * For amends we only need to confirm the original clOrdId exists.
     *
     * @return VALID, or ERR_DUPLICATE_CL_ORD_ID if the new clOrdId was already seen,
     *         or a positive code if the original order is not found (re-use INVALID_ACCOUNT
     *         as "ORDER_NOT_FOUND" sentinel here for brevity).
     */
    public int validateCancelRequest(final OrderFlyweight order) {
        // New ClOrdId on the cancel must be fresh
        if (seenClOrdIds.get(order.clOrdId()) != DEDUP_MISSING) {
            return ERR_DUPLICATE_CL_ORD_ID;
        }
        // origClOrdId must refer to a live order
        if (seenClOrdIds.get(order.origClOrdId()) == DEDUP_MISSING) {
            return ERR_INVALID_ACCOUNT; // reusing as "original order not found"
        }
        return VALID;
    }

    // ── De-duplication lifecycle ───────────────────────────────────────────────

    /**
     * Register an accepted order in the dedup map.
     * Called AFTER the order has been written to the OrderBook and the state machine
     * has moved it to PENDING_NEW — i.e., only for orders that passed validation.
     *
     * ZERO-GC: single Long2LongHashMap.put(long, long).
     */
    public void registerAccepted(final long clOrdId, final long orderId) {
        seenClOrdIds.put(clOrdId, orderId);
    }

    /**
     * Remove a terminal order from the dedup map.
     * Optional; only called if we want to reclaim map memory for very-long-running sessions.
     * In a typical session, we leave terminal orders in the map to prevent replay attacks.
     */
    public void deregister(final long clOrdId) {
        seenClOrdIds.remove(clOrdId);
    }

    // ── Snapshot support ──────────────────────────────────────────────────────

    /** Expose the dedup map to SnapshotManager for serialization. Off hot path. */
    public Long2LongHashMap seenClOrdIds() { return seenClOrdIds; }

    /** Clear and rebuild dedup state from a snapshot. Off hot path. */
    public void reset() { seenClOrdIds.clear(); }

    /**
     * Restore a single dedup entry during snapshot load.
     * Called once per entry by SnapshotManager.
     */
    public void restoreEntry(final long clOrdId, final long orderId) {
        seenClOrdIds.put(clOrdId, orderId);
    }

    // ── Human-readable error — off hot path, for logging only ─────────────────

    public static String errorName(final int code) {
        return switch (code) {
            case VALID                      -> "VALID";
            case ERR_DUPLICATE_CL_ORD_ID   -> "DUPLICATE_CL_ORD_ID";
            case ERR_INVALID_SIDE           -> "INVALID_SIDE";
            case ERR_INVALID_TIF            -> "INVALID_TIF";
            case ERR_INVALID_QTY            -> "INVALID_QTY";
            case ERR_INVALID_PRICE          -> "INVALID_PRICE";
            case ERR_SYMBOL_NOT_WHITELISTED -> "SYMBOL_NOT_WHITELISTED";
            case ERR_NOTIONAL_LIMIT         -> "NOTIONAL_LIMIT_EXCEEDED";
            case ERR_INVALID_ACCOUNT        -> "INVALID_ACCOUNT_OR_ORDER_NOT_FOUND";
            default                         -> "UNKNOWN(" + code + ")";
        };
    }
}
