package com.cobain.oms.harness.perf;

import com.cobain.oms.model.OrderFlyweight;
import com.cobain.oms.model.OrderLayout;
import com.cobain.oms.model.Side;
import io.aeron.cluster.client.AeronCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Measures single-order round-trip latency with warmup, GC tracking, and percentile reporting.
 *
 * Protocol:
 *   1. Warmup: 10 000 orders at PERF_SEND_INTERVAL_NS, results discarded
 *   2. snapshotGcBefore()
 *   3. Measurement: PERF_SAMPLE_COUNT orders at PERF_SEND_INTERVAL_NS
 *   4. snapshotGcAfter()
 *   5. Drain: poll egress until outstanding == 0 or 10s timeout
 *   6. Report histogram and GC delta
 *   7. Assert p99 <= PERF_P99_LIMIT_US; optionally assert GC == 0
 */
public final class LatencyBenchmark {

    private static final Logger log = LoggerFactory.getLogger(LatencyBenchmark.class);

    private static final int    WARMUP_COUNT     = 10_000;
    private static final long   DRAIN_TIMEOUT_MS = 10_000L;
    private static final long   ACCOUNT_ID       = 42_001L;
    private static final long   PRICE_FIXED_PT   = 150_0000L;
    private static final long   QTY              = 100L;

    private static final long SYMBOL_AAPL = OrderFlyweight.encodeSymbol("AAPL");

    private final PerfConfig              config;
    private final AeronCluster            cluster;
    private final PerfOrderInjector       injector;
    private final PerfExecReportListener  listener;
    private final LatencyHistogram        histogram;

    public LatencyBenchmark(
            final PerfConfig             config,
            final AeronCluster           cluster,
            final PerfOrderInjector      injector,
            final PerfExecReportListener listener,
            final LatencyHistogram       histogram) {
        this.config    = config;
        this.cluster   = cluster;
        this.injector  = injector;
        this.listener  = listener;
        this.histogram = histogram;
    }

    public BenchmarkResult run() {
        log.info("=== LatencyBenchmark starting: warmup={} measurement={}",
                WARMUP_COUNT, config.sampleCount);

        listener.reset();
        histogram.reset();

        // ── 1. Warmup ─────────────────────────────────────────────────────────
        log.info("  Warmup ({} orders)...", WARMUP_COUNT);
        long nextSend = System.nanoTime();
        for (int i = 0; i < WARMUP_COUNT; i++) {
            nextSend += config.sendIntervalNs;
            PerfOrderInjector.awaitNextSend(nextSend);
            injector.inject(ACCOUNT_ID, SYMBOL_AAPL, Side.BUY, PRICE_FIXED_PT, QTY, true);
            listener.poll(cluster);
        }
        // Drain warmup responses
        drainUntilQuiet(cluster, listener, 3_000L);

        // Reset counters after warmup so measurement is clean
        listener.reset();
        histogram.reset();

        // ── 2. GC before ─────────────────────────────────────────────────────
        listener.snapshotGcBefore();

        // ── 3. Measurement ────────────────────────────────────────────────────
        log.info("  Measurement ({} orders)...", config.sampleCount);
        nextSend = System.nanoTime();
        for (long i = 0; i < config.sampleCount; i++) {
            nextSend += config.sendIntervalNs;
            PerfOrderInjector.awaitNextSend(nextSend);
            injector.inject(ACCOUNT_ID, SYMBOL_AAPL, Side.BUY, PRICE_FIXED_PT, QTY, false);
            listener.recordSent();
            listener.poll(cluster);
        }

        // ── 4. GC after ──────────────────────────────────────────────────────
        listener.snapshotGcAfter();

        // ── 5. Drain ─────────────────────────────────────────────────────────
        log.info("  Draining responses (outstanding={})...", listener.outstanding());
        final long drainDeadline = System.currentTimeMillis() + DRAIN_TIMEOUT_MS;
        while (listener.outstanding() > 0 && System.currentTimeMillis() < drainDeadline) {
            listener.poll(cluster);
            Thread.onSpinWait();
        }
        if (listener.outstanding() > 0) {
            log.warn("  Drain timeout: {} orders still outstanding", listener.outstanding());
        }

        // ── 6. Report ─────────────────────────────────────────────────────────
        final long p50    = histogram.percentile(50.0);
        final long p90    = histogram.percentile(90.0);
        final long p99    = histogram.percentile(99.0);
        final long p999   = histogram.percentile(99.9);
        final long p9999  = histogram.percentile(99.99);
        final long maxNs  = histogram.max();
        final long gcDelta = listener.gcCollectionsDelta();
        final long gcMs    = listener.gcTimeDeltaMs();

        log.info("  Results:");
        histogram.print(System.out);
        log.info("  GC collections during measurement: {} ({} ms)", gcDelta, gcMs);

        // ── 7. Assertions ─────────────────────────────────────────────────────
        boolean passed = true;
        final long p99LimitNs = config.p99LimitUs * 1_000L;
        if (p99 > p99LimitNs) {
            log.warn("  FAIL: p99 {} µs > limit {} µs", p99 / 1_000, config.p99LimitUs);
            passed = false;
        }
        if (config.assertZeroGc && gcDelta > 0) {
            log.warn("  FAIL: {} GC collections during measurement (PERF_ASSERT_ZERO_GC=true)", gcDelta);
            passed = false;
        } else if (gcDelta > 0) {
            log.warn("  WARN: {} GC collections during measurement", gcDelta);
        }

        final Map<String, String> metrics = new LinkedHashMap<>();
        metrics.put("p50_us",            String.valueOf(p50 / 1_000));
        metrics.put("p90_us",            String.valueOf(p90 / 1_000));
        metrics.put("p99_us",            String.valueOf(p99 / 1_000));
        metrics.put("p99_9_us",          String.valueOf(p999 / 1_000));
        metrics.put("p99_99_us",         String.valueOf(p9999 / 1_000));
        metrics.put("max_us",            String.valueOf(maxNs / 1_000));
        metrics.put("gc_collections",    String.valueOf(gcDelta));
        metrics.put("gc_time_ms",        String.valueOf(gcMs));
        metrics.put("sample_count",      String.valueOf(histogram.totalCount()));

        log.info("  LatencyBenchmark: {}", passed ? "PASS" : "FAIL");
        return new BenchmarkResult("LatencyBenchmark", passed, metrics);
    }

    private static void drainUntilQuiet(
            final AeronCluster cluster,
            final PerfExecReportListener listener,
            final long timeoutMs) {
        final long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            listener.poll(cluster);
            Thread.onSpinWait();
        }
    }
}
