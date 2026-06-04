package com.cobain.oms.harness.perf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Immutable, validated snapshot of all PERF_* performance-harness configuration.
 *
 * Mirrors the OmsConfig pattern: reads env vars, validates, logs each resolved field at INFO
 * with its source name. Does not depend on oms-config to avoid making oms-config a harness dep.
 *
 * Validation: all integer/long values >= 1; ORDER_PCT + CANCEL_PCT + REPLACE_PCT must sum to 100.
 * Fails fast with IllegalArgumentException on any invalid value.
 */
public final class PerfConfig {

    private static final Logger log = LoggerFactory.getLogger(PerfConfig.class);

    // ── Env-var key constants ─────────────────────────────────────────────────
    public static final String KEY_SAMPLE_COUNT           = "PERF_SAMPLE_COUNT";
    public static final String KEY_SEND_INTERVAL_NS       = "PERF_SEND_INTERVAL_NS";
    public static final String KEY_P99_LIMIT_US           = "PERF_P99_LIMIT_US";
    public static final String KEY_ASSERT_ZERO_GC         = "PERF_ASSERT_ZERO_GC";
    public static final String KEY_MAX_OUTSTANDING        = "PERF_MAX_OUTSTANDING";
    public static final String KEY_THROUGHPUT_TIMEOUT_MS  = "PERF_THROUGHPUT_TIMEOUT_MS";
    public static final String KEY_DURATION_SECONDS       = "PERF_DURATION_SECONDS";
    public static final String KEY_ORDER_PCT              = "PERF_ORDER_PCT";
    public static final String KEY_CANCEL_PCT             = "PERF_CANCEL_PCT";
    public static final String KEY_REPLACE_PCT            = "PERF_REPLACE_PCT";
    public static final String KEY_MAX_LOSS_PCT           = "PERF_MAX_LOSS_PCT";
    public static final String KEY_SUSTAINED_DURATION_S   = "PERF_SUSTAINED_DURATION_S";
    public static final String KEY_PARENT_ORDER_COUNT     = "PERF_PARENT_ORDER_COUNT";
    public static final String KEY_SNAPSHOT_RESUME_LIMIT_MS = "PERF_SNAPSHOT_RESUME_LIMIT_MS";

    // ── Defaults ──────────────────────────────────────────────────────────────
    public static final long    DEFAULT_SAMPLE_COUNT           = 100_000L;
    public static final long    DEFAULT_SEND_INTERVAL_NS       = 100_000L;
    public static final long    DEFAULT_P99_LIMIT_US           = 1_000L;
    public static final boolean DEFAULT_ASSERT_ZERO_GC         = false;
    public static final int     DEFAULT_MAX_OUTSTANDING        = 500;
    public static final long    DEFAULT_THROUGHPUT_TIMEOUT_MS  = 5_000L;
    public static final int     DEFAULT_DURATION_SECONDS       = 30;
    public static final int     DEFAULT_ORDER_PCT              = 70;
    public static final int     DEFAULT_CANCEL_PCT             = 20;
    public static final int     DEFAULT_REPLACE_PCT            = 10;
    public static final double  DEFAULT_MAX_LOSS_PCT           = 0.01;
    public static final int     DEFAULT_SUSTAINED_DURATION_S   = 60;
    public static final int     DEFAULT_PARENT_ORDER_COUNT     = 500;
    public static final long    DEFAULT_SNAPSHOT_RESUME_LIMIT_MS = 5_000L;

    // ── Fields ────────────────────────────────────────────────────────────────
    /** Number of orders to send in the latency measurement phase. */
    public final long sampleCount;

    /** Inter-message interval in nanoseconds for rate-controlled injection. */
    public final long sendIntervalNs;

    /** p99 latency threshold in microseconds; fail if exceeded. */
    public final long p99LimitUs;

    /** If true, fail when GC collections occur during measurement. */
    public final boolean assertZeroGc;

    /** Maximum allowed in-flight (outstanding) orders at any instant. */
    public final int maxOutstanding;

    /** Timeout to wait for outstanding orders to drain after injection stops (ms). */
    public final long throughputTimeoutMs;

    /** Duration of the throughput benchmark in seconds. */
    public final int durationSeconds;

    /** Percentage of iterations that send a NEW order (must total 100 with cancel+replace). */
    public final int orderPct;

    /** Percentage of iterations that send a CANCEL. */
    public final int cancelPct;

    /** Percentage of iterations that send a REPLACE. */
    public final int replacePct;

    /** Maximum acceptable message loss fraction (0.01 = 1%). */
    public final double maxLossPct;

    /** Duration of the sustained-load scenario in seconds. */
    public final int sustainedDurationS;

    /** Number of parent orders to inject in ChildOrderRegistrySaturationTest. */
    public final int parentOrderCount;

    /** Maximum acceptable snapshot-to-resume latency in milliseconds. */
    public final long snapshotResumeLimitMs;

    private PerfConfig(
            final long sampleCount,
            final long sendIntervalNs,
            final long p99LimitUs,
            final boolean assertZeroGc,
            final int maxOutstanding,
            final long throughputTimeoutMs,
            final int durationSeconds,
            final int orderPct,
            final int cancelPct,
            final int replacePct,
            final double maxLossPct,
            final int sustainedDurationS,
            final int parentOrderCount,
            final long snapshotResumeLimitMs) {
        this.sampleCount           = sampleCount;
        this.sendIntervalNs        = sendIntervalNs;
        this.p99LimitUs            = p99LimitUs;
        this.assertZeroGc          = assertZeroGc;
        this.maxOutstanding        = maxOutstanding;
        this.throughputTimeoutMs   = throughputTimeoutMs;
        this.durationSeconds       = durationSeconds;
        this.orderPct              = orderPct;
        this.cancelPct             = cancelPct;
        this.replacePct            = replacePct;
        this.maxLossPct            = maxLossPct;
        this.sustainedDurationS    = sustainedDurationS;
        this.parentOrderCount      = parentOrderCount;
        this.snapshotResumeLimitMs = snapshotResumeLimitMs;
    }

    /** Reads env vars, validates, and returns a fully-resolved PerfConfig. */
    public static PerfConfig load() {
        final long    sampleCount           = longEnv(KEY_SAMPLE_COUNT,           DEFAULT_SAMPLE_COUNT);
        final long    sendIntervalNs        = longEnv(KEY_SEND_INTERVAL_NS,       DEFAULT_SEND_INTERVAL_NS);
        final long    p99LimitUs            = longEnv(KEY_P99_LIMIT_US,           DEFAULT_P99_LIMIT_US);
        final boolean assertZeroGc          = boolEnv(KEY_ASSERT_ZERO_GC,         DEFAULT_ASSERT_ZERO_GC);
        final int     maxOutstanding        = intEnv (KEY_MAX_OUTSTANDING,        DEFAULT_MAX_OUTSTANDING);
        final long    throughputTimeoutMs   = longEnv(KEY_THROUGHPUT_TIMEOUT_MS,  DEFAULT_THROUGHPUT_TIMEOUT_MS);
        final int     durationSeconds       = intEnv (KEY_DURATION_SECONDS,       DEFAULT_DURATION_SECONDS);
        final int     orderPct              = intEnv (KEY_ORDER_PCT,              DEFAULT_ORDER_PCT);
        final int     cancelPct             = intEnv (KEY_CANCEL_PCT,             DEFAULT_CANCEL_PCT);
        final int     replacePct            = intEnv (KEY_REPLACE_PCT,            DEFAULT_REPLACE_PCT);
        final double  maxLossPct            = doubleEnv(KEY_MAX_LOSS_PCT,         DEFAULT_MAX_LOSS_PCT);
        final int     sustainedDurationS    = intEnv (KEY_SUSTAINED_DURATION_S,   DEFAULT_SUSTAINED_DURATION_S);
        final int     parentOrderCount      = intEnv (KEY_PARENT_ORDER_COUNT,     DEFAULT_PARENT_ORDER_COUNT);
        final long    snapshotResumeLimitMs = longEnv(KEY_SNAPSHOT_RESUME_LIMIT_MS, DEFAULT_SNAPSHOT_RESUME_LIMIT_MS);

        validate(sampleCount,           KEY_SAMPLE_COUNT,           1L);
        validate(sendIntervalNs,        KEY_SEND_INTERVAL_NS,       1L);
        validate(p99LimitUs,            KEY_P99_LIMIT_US,           1L);
        validate(maxOutstanding,        KEY_MAX_OUTSTANDING,        1);
        validate(throughputTimeoutMs,   KEY_THROUGHPUT_TIMEOUT_MS,  1L);
        validate(durationSeconds,       KEY_DURATION_SECONDS,       1);
        validate(sustainedDurationS,    KEY_SUSTAINED_DURATION_S,   1);
        validate(parentOrderCount,      KEY_PARENT_ORDER_COUNT,     1);
        validate(snapshotResumeLimitMs, KEY_SNAPSHOT_RESUME_LIMIT_MS, 1L);

        if (orderPct + cancelPct + replacePct != 100) {
            throw new IllegalArgumentException(
                    KEY_ORDER_PCT + " + " + KEY_CANCEL_PCT + " + " + KEY_REPLACE_PCT +
                    " must sum to 100, got " + (orderPct + cancelPct + replacePct));
        }

        log.info("PerfConfig resolved:");
        log.info("  {} = {}", KEY_SAMPLE_COUNT,           sampleCount);
        log.info("  {} = {} ns", KEY_SEND_INTERVAL_NS,    sendIntervalNs);
        log.info("  {} = {} µs", KEY_P99_LIMIT_US,        p99LimitUs);
        log.info("  {} = {}", KEY_ASSERT_ZERO_GC,         assertZeroGc);
        log.info("  {} = {}", KEY_MAX_OUTSTANDING,        maxOutstanding);
        log.info("  {} = {} ms", KEY_THROUGHPUT_TIMEOUT_MS, throughputTimeoutMs);
        log.info("  {} = {} s", KEY_DURATION_SECONDS,     durationSeconds);
        log.info("  {} / {} / {} = {}/{}/{}", KEY_ORDER_PCT, KEY_CANCEL_PCT, KEY_REPLACE_PCT,
                orderPct, cancelPct, replacePct);
        log.info("  {} = {}", KEY_MAX_LOSS_PCT,           maxLossPct);
        log.info("  {} = {} s", KEY_SUSTAINED_DURATION_S, sustainedDurationS);
        log.info("  {} = {}", KEY_PARENT_ORDER_COUNT,     parentOrderCount);
        log.info("  {} = {} ms", KEY_SNAPSHOT_RESUME_LIMIT_MS, snapshotResumeLimitMs);

        return new PerfConfig(sampleCount, sendIntervalNs, p99LimitUs, assertZeroGc,
                maxOutstanding, throughputTimeoutMs, durationSeconds, orderPct, cancelPct,
                replacePct, maxLossPct, sustainedDurationS, parentOrderCount, snapshotResumeLimitMs);
    }

    private static void validate(final long value, final String key, final long min) {
        if (value < min) {
            throw new IllegalArgumentException(key + " must be >= " + min + ", got " + value);
        }
    }

    private static void validate(final int value, final String key, final int min) {
        if (value < min) {
            throw new IllegalArgumentException(key + " must be >= " + min + ", got " + value);
        }
    }

    private static long longEnv(final String key, final long defaultValue) {
        final String v = System.getenv(key);
        return (v != null && !v.isEmpty()) ? Long.parseLong(v.trim()) : defaultValue;
    }

    private static int intEnv(final String key, final int defaultValue) {
        final String v = System.getenv(key);
        return (v != null && !v.isEmpty()) ? Integer.parseInt(v.trim()) : defaultValue;
    }

    private static boolean boolEnv(final String key, final boolean defaultValue) {
        final String v = System.getenv(key);
        return (v != null && !v.isEmpty()) ? Boolean.parseBoolean(v.trim()) : defaultValue;
    }

    private static double doubleEnv(final String key, final double defaultValue) {
        final String v = System.getenv(key);
        return (v != null && !v.isEmpty()) ? Double.parseDouble(v.trim()) : defaultValue;
    }
}
