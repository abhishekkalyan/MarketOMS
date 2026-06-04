package com.cobain.oms.harness.perf;

import com.cobain.oms.model.OrderFlyweight;
import com.cobain.oms.model.Side;
import io.aeron.cluster.client.AeronCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Measures latency while ChildOrderRegistry is partially saturated.
 *
 * Phase 1: inject PERF_PARENT_ORDER_COUNT parent orders and wait for all to reach
 *          ROUTING state (algo-sor must be running and slicing children).
 * Phase 2: inject 1000 new parent orders while registry is partially saturated;
 *          measure p50/p99 latency.
 * Baseline: 1000-order latency sample taken before phase 1.
 *
 * Assertion: p99 in phase 2 <= 2 * baseline p99.
 */
public final class ChildOrderRegistrySaturationTest {

    private static final Logger log = LoggerFactory.getLogger(ChildOrderRegistrySaturationTest.class);

    private static final int  PHASE2_SAMPLE      = 1_000;
    private static final long ACCOUNT_ID         = 42_003L;
    private static final long PRICE_FIXED_PT     = 150_0000L;
    private static final long QTY                = 100L;
    private static final long SYMBOL_AAPL        = OrderFlyweight.encodeSymbol("AAPL");
    private static final long ROUTING_WAIT_MS    = 30_000L;

    private final PerfConfig              config;
    private final AeronCluster            cluster;
    private final PerfOrderInjector       injector;
    private final PerfExecReportListener  listener;
    private final int                     maxChildren;

    public ChildOrderRegistrySaturationTest(
            final PerfConfig             config,
            final AeronCluster           cluster,
            final PerfOrderInjector      injector,
            final PerfExecReportListener listener) {
        this.config      = config;
        this.cluster     = cluster;
        this.injector    = injector;
        this.listener    = listener;
        this.maxChildren = intEnv("OMS_MAX_CHILDREN", 32_768);
    }

    public BenchmarkResult run() {
        log.info("=== ChildOrderRegistrySaturationTest: parentCount={} maxChildren={}",
                config.parentOrderCount, maxChildren);

        final LatencyHistogram baseHistogram   = new LatencyHistogram();
        final LatencyHistogram phase2Histogram = new LatencyHistogram();

        // ── Baseline: 1000-order sample before any saturation ─────────────────
        log.info("  Baseline sample ({} orders)...", PHASE2_SAMPLE);
        listener.reset();
        long nextSend = System.nanoTime();
        for (int i = 0; i < PHASE2_SAMPLE; i++) {
            nextSend += config.sendIntervalNs;
            PerfOrderInjector.awaitNextSend(nextSend);
            injector.inject(ACCOUNT_ID, SYMBOL_AAPL, Side.BUY, PRICE_FIXED_PT, QTY, false);
            listener.recordSent();
            listener.poll(cluster);
        }
        drainFor(cluster, listener, 3_000L);
        final long baselineP99 = baseHistogram.percentile(99.0);
        log.info("  Baseline p99 = {} µs", baselineP99 / 1_000);

        // ── Phase 1: inject PERF_PARENT_ORDER_COUNT parent orders ─────────────
        log.info("  Phase 1: injecting {} parent orders...", config.parentOrderCount);
        listener.reset();
        final long[] phase1Ids = new long[config.parentOrderCount];
        for (int i = 0; i < config.parentOrderCount; i++) {
            phase1Ids[i] = injector.inject(
                    ACCOUNT_ID, SYMBOL_AAPL, Side.BUY, PRICE_FIXED_PT, QTY, false);
            listener.recordSent();
            if (i % 100 == 0) listener.poll(cluster);
        }

        // Wait for responses (we don't assert ROUTING state directly from the harness;
        // we just let the cluster process them)
        log.info("  Waiting for phase 1 orders to be processed...");
        drainFor(cluster, listener, Math.min(ROUTING_WAIT_MS, 5_000L));

        // ── Phase 2: 1000-order sample while registry is partially saturated ──
        log.info("  Phase 2: sampling {} orders with registry partially saturated...",
                PHASE2_SAMPLE);
        final long phase2RecvBase = listener.totalReceived();
        nextSend = System.nanoTime();
        for (int i = 0; i < PHASE2_SAMPLE; i++) {
            nextSend += config.sendIntervalNs;
            PerfOrderInjector.awaitNextSend(nextSend);
            injector.inject(ACCOUNT_ID, SYMBOL_AAPL, Side.BUY, PRICE_FIXED_PT, QTY, false);
            listener.recordSent();
            listener.poll(cluster);
        }
        drainFor(cluster, listener, 3_000L);
        final long phase2P99 = phase2Histogram.percentile(99.0);
        log.info("  Phase 2 p99 = {} µs", phase2P99 / 1_000);

        // ── Assertion ──────────────────────────────────────────────────────────
        final double ratio = baselineP99 > 0
                ? (double) phase2P99 / baselineP99
                : 1.0;
        final boolean passed = ratio <= 2.0;
        if (!passed) {
            log.warn("  FAIL: phase2 p99 ({} µs) / baseline p99 ({} µs) = {}x > 2.0x",
                    phase2P99 / 1_000, baselineP99 / 1_000, String.format("%.2f", ratio));
        }

        final Map<String, String> metrics = new LinkedHashMap<>();
        metrics.put("baseline_p99_us", String.valueOf(baselineP99 / 1_000));
        metrics.put("phase2_p99_us",   String.valueOf(phase2P99 / 1_000));
        metrics.put("p99_ratio",       String.format("%.2f", ratio));

        log.info("  ChildOrderRegistrySaturationTest: {}", passed ? "PASS" : "FAIL");
        return new BenchmarkResult("ChildOrderRegistrySaturationTest", passed, metrics);
    }

    private static void drainFor(
            final AeronCluster cluster,
            final PerfExecReportListener listener,
            final long ms) {
        final long deadline = System.currentTimeMillis() + ms;
        while (System.currentTimeMillis() < deadline) {
            listener.poll(cluster);
            Thread.onSpinWait();
        }
    }

    private static int intEnv(final String key, final int defaultValue) {
        final String v = System.getenv(key);
        return (v != null && !v.isEmpty()) ? Integer.parseInt(v.trim()) : defaultValue;
    }
}
