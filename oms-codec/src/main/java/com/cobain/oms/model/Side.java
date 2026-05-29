package com.cobain.oms.model;

/**
 * FIX Side field (tag 54) encoded as primitive bytes.
 * Values match the FIX 4.4 specification exactly so the decoder can copy
 * the parsed byte directly into OrderLayout.SIDE_OFFSET without translation.
 */
public final class Side {

    public static final byte BUY         = 1;
    public static final byte SELL        = 2;
    public static final byte SELL_SHORT  = 5;

    /** Fast O(1) validity check — no boxing, no iterator. */
    public static boolean isValid(byte side) {
        return side == BUY || side == SELL || side == SELL_SHORT;
    }

    public static String nameOf(byte side) {
        return switch (side) {
            case BUY        -> "BUY";
            case SELL       -> "SELL";
            case SELL_SHORT -> "SELL_SHORT";
            default         -> "UNKNOWN(" + side + ")";
        };
    }

    private Side() {}
}
