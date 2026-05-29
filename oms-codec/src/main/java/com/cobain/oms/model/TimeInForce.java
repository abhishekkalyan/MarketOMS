package com.cobain.oms.model;

/**
 * FIX TimeInForce field (tag 59) — primitive byte constants.
 * Values match FIX 4.4 specification directly.
 */
public final class TimeInForce {

    public static final byte DAY  = 0;  // expires at end of trading day
    public static final byte GTC  = 1;  // Good Till Canceled
    public static final byte IOC  = 3;  // Immediate Or Cancel
    public static final byte FOK  = 4;  // Fill Or Kill
    public static final byte GTD  = 6;  // Good Till Date

    public static boolean isValid(byte tif) {
        return tif == DAY || tif == GTC || tif == IOC || tif == FOK || tif == GTD;
    }

    public static String nameOf(byte tif) {
        return switch (tif) {
            case DAY -> "DAY";
            case GTC -> "GTC";
            case IOC -> "IOC";
            case FOK -> "FOK";
            case GTD -> "GTD";
            default  -> "UNKNOWN(" + tif + ")";
        };
    }

    private TimeInForce() {}
}
