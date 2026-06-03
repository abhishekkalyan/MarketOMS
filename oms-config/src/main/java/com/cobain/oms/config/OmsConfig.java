package com.cobain.oms.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Immutable, validated snapshot of all OMS capacity and tuning configuration.
 *
 * <h2>Single source of truth</h2>
 * All capacity limits and tuning constants that were previously read ad-hoc in
 * {@code OmsNode} and {@code OmsLauncher} are now resolved once here and injected
 * as constructor arguments into the components that need them.
 *
 * <h2>Validation</h2>
 * Every field is validated on construction. An invalid value (zero, negative,
 * unparseable, or blank symbol list) causes an {@link IllegalArgumentException}
 * with a clear message before any Aeron infrastructure is started.
 *
 * <h2>Startup logging</h2>
 * Every resolved field is logged at INFO with its value and the name of the
 * {@link ConfigSource} that supplied it (or {@code "default"} when the built-in
 * default was used). This makes it trivially auditable which values came from a
 * properties file vs. the environment.
 *
 * <h2>Env-var names are unchanged</h2>
 * The environment variable names ({@code OMS_MAX_ORDERS}, etc.) are preserved so
 * that existing shell scripts and deployment tooling work without modification.
 */
public final class OmsConfig {

    // ── Env-var key constants ────────────────────────────────────────────────
    public static final String KEY_MAX_ORDERS             = "OMS_MAX_ORDERS";
    public static final String KEY_MAX_CHILDREN           = "OMS_MAX_CHILDREN";
    public static final String KEY_INTENT_FRAGMENT_LIMIT  = "OMS_INTENT_FRAGMENT_LIMIT";
    public static final String KEY_ALGO_FRAGMENT_LIMIT    = "OMS_ALGO_FRAGMENT_LIMIT";
    public static final String KEY_MAX_VENUES             = "OMS_MAX_VENUES";
    public static final String KEY_MAX_NOTIONAL           = "OMS_MAX_NOTIONAL";
    public static final String KEY_SYMBOLS                = "OMS_SYMBOLS";
    /** Optional path to a {@code .properties} config file. Read by entry points, not by this class. */
    public static final String KEY_CONFIG_FILE            = "OMS_CONFIG_FILE";

    // ── Hard-coded defaults ──────────────────────────────────────────────────
    public static final int    DEFAULT_MAX_ORDERS             = 65_536;
    public static final int    DEFAULT_MAX_CHILDREN           = 32_768;
    public static final int    DEFAULT_INTENT_FRAGMENT_LIMIT  = 20;
    public static final int    DEFAULT_ALGO_FRAGMENT_LIMIT    = 10;
    public static final int    DEFAULT_MAX_VENUES             = 10;
    public static final long   DEFAULT_MAX_NOTIONAL           = 10_000_000L;
    public static final String DEFAULT_SYMBOLS                = "AAPL,MSFT,GOOG,AMZN";

    // ── Fields ───────────────────────────────────────────────────────────────
    /** Maximum parent orders held in {@code OrderBook} (off-heap pre-allocation). */
    public final int maxOrders;

    /** Maximum child orders held in {@code ChildOrderRegistry} (off-heap pre-allocation). */
    public final int maxChildren;

    /**
     * Maximum {@code ChildOrderIntent} fragments polled per work cycle by
     * {@code OmsClusteredService}.
     */
    public final int intentFragmentLimit;

    /** Maximum parent-order fragments polled per work cycle by {@code AlgoSorAgent}. */
    public final int algoFragmentLimit;

    /**
     * Maximum venues {@code SmartOrderRouter} can distribute across.
     * Venue arrays are pre-allocated at this size in the constructor.
     */
    public final int maxVenues;

    /** Per-order notional limit in base currency units (fixed-point, multiplied by {@code PRICE_MULTIPLIER}). */
    public final long maxNotional;

    /** Comma-separated permitted symbol whitelist (e.g. {@code "AAPL,MSFT,GOOG,AMZN"}). */
    public final String symbols;

    // ── Private constructor — use load() ────────────────────────────────────

    private OmsConfig(
            final int    maxOrders,
            final int    maxChildren,
            final int    intentFragmentLimit,
            final int    algoFragmentLimit,
            final int    maxVenues,
            final long   maxNotional,
            final String symbols) {
        this.maxOrders            = maxOrders;
        this.maxChildren          = maxChildren;
        this.intentFragmentLimit  = intentFragmentLimit;
        this.algoFragmentLimit    = algoFragmentLimit;
        this.maxVenues            = maxVenues;
        this.maxNotional          = maxNotional;
        this.symbols              = symbols;
    }

    // ── Factory ──────────────────────────────────────────────────────────────

    /**
     * Resolve, validate, and log all configuration fields from {@code source}.
     *
     * <p>Each field is logged at INFO as:
     * <pre>
     *   [OmsConfig] OMS_MAX_ORDERS = 65536  (source: env)
     * </pre>
     *
     * @param source the {@link ConfigSource} to query — typically a
     *               {@link ChainedConfigSource} that tries a properties file
     *               first, then falls back to env vars
     * @return validated, immutable {@code OmsConfig}
     * @throws IllegalArgumentException if any value is invalid (zero, negative,
     *                                  non-integer where an integer is expected,
     *                                  or a blank/empty symbol list)
     */
    public static OmsConfig load(final ConfigSource source) {
        final Logger log = LoggerFactory.getLogger(OmsConfig.class);

        final int    maxOrders            = resolveInt(source,  KEY_MAX_ORDERS,            DEFAULT_MAX_ORDERS,            1, Integer.MAX_VALUE, log);
        final int    maxChildren          = resolveInt(source,  KEY_MAX_CHILDREN,           DEFAULT_MAX_CHILDREN,          1, Integer.MAX_VALUE, log);
        final int    intentFragmentLimit  = resolveInt(source,  KEY_INTENT_FRAGMENT_LIMIT,  DEFAULT_INTENT_FRAGMENT_LIMIT, 1, Integer.MAX_VALUE, log);
        final int    algoFragmentLimit    = resolveInt(source,  KEY_ALGO_FRAGMENT_LIMIT,    DEFAULT_ALGO_FRAGMENT_LIMIT,   1, Integer.MAX_VALUE, log);
        final int    maxVenues            = resolveInt(source,  KEY_MAX_VENUES,             DEFAULT_MAX_VENUES,            1, Integer.MAX_VALUE, log);
        final long   maxNotional          = resolveLong(source, KEY_MAX_NOTIONAL,           DEFAULT_MAX_NOTIONAL,          1L, Long.MAX_VALUE,    log);
        final String symbols              = resolveString(source, KEY_SYMBOLS,              DEFAULT_SYMBOLS,               log);

        return new OmsConfig(maxOrders, maxChildren, intentFragmentLimit,
                algoFragmentLimit, maxVenues, maxNotional, symbols);
    }

    // ── Resolution helpers ───────────────────────────────────────────────────

    private static int resolveInt(
            final ConfigSource source,
            final String       key,
            final int          defaultValue,
            final int          min,
            final int          max,
            final Logger       log) {

        final String raw        = source.get(key);
        final String sourceName = source.sourceName(key);

        final int value;
        if (raw == null) {
            value = defaultValue;
        } else {
            try {
                value = Integer.parseInt(raw.trim());
            } catch (final NumberFormatException e) {
                throw new IllegalArgumentException(
                        key + " must be a valid integer but was '" + raw + "'", e);
            }
        }

        if (value < min || value > max) {
            throw new IllegalArgumentException(
                    key + " must be between " + min + " and " + max + " but was " + value);
        }

        log.info("[OmsConfig] {} = {}  (source: {})", key, value, sourceName);
        return value;
    }

    private static long resolveLong(
            final ConfigSource source,
            final String       key,
            final long         defaultValue,
            final long         min,
            final long         max,
            final Logger       log) {

        final String raw        = source.get(key);
        final String sourceName = source.sourceName(key);

        final long value;
        if (raw == null) {
            value = defaultValue;
        } else {
            try {
                value = Long.parseLong(raw.trim());
            } catch (final NumberFormatException e) {
                throw new IllegalArgumentException(
                        key + " must be a valid long integer but was '" + raw + "'", e);
            }
        }

        if (value < min || value > max) {
            throw new IllegalArgumentException(
                    key + " must be between " + min + " and " + max + " but was " + value);
        }

        log.info("[OmsConfig] {} = {}  (source: {})", key, value, sourceName);
        return value;
    }

    private static String resolveString(
            final ConfigSource source,
            final String       key,
            final String       defaultValue,
            final Logger       log) {

        final String raw        = source.get(key);
        final String sourceName = source.sourceName(key);

        final String value = (raw != null && !raw.trim().isEmpty()) ? raw.trim() : defaultValue;

        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(key + " must not be blank");
        }

        log.info("[OmsConfig] {} = {}  (source: {})", key, value, sourceName);
        return value;
    }

    @Override
    public String toString() {
        return "OmsConfig{" +
                "maxOrders=" + maxOrders +
                ", maxChildren=" + maxChildren +
                ", intentFragmentLimit=" + intentFragmentLimit +
                ", algoFragmentLimit=" + algoFragmentLimit +
                ", maxVenues=" + maxVenues +
                ", maxNotional=" + maxNotional +
                ", symbols='" + symbols + '\'' +
                '}';
    }
}
