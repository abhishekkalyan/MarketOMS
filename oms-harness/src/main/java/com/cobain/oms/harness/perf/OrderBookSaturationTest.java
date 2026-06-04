package com.cobain.oms.harness.perf;

import com.cobain.oms.model.OrderFlyweight;
import com.cobain.oms.model.OrderState;
import com.cobain.oms.model.Side;
import io.aeron.cluster.client.AeronCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Fills OrderBook to 25%, 50%, 75%, 90% of OMS_MAX_ORDERS and samples 1000-order latency
 * at each level.
 *
 * <p><b>Assumption:</b> OmsNode has no venue stub sending fills back. Orders injected here
 * stay live (NEW state) without transitioning to FILLED. If fills arrive the live count will
 * be lower than expected and the test may not reach the stated fill levels.
 *
 * <p>After all levels, cancels all injected orders in reverse order (awaits each cancel
 * within a 10s total timeout).
 *
 * <p><b>Assertion:</b> p99 at 90% fill <= p99 at 25% fill * 2.0
 */
public final class OrderBookSaturationTest {

    private static final Logger log = LoggerFactory.getLogger(OrderBookSaturationTest.class);

    private static final double[] FILL_LEVELS     = {0.25, 0.50, 0.75, 0.90};
    private static final int      LATENCY_SAMPLE  = 1_000;
    private static final long     DRAIN_TIMEOUT_MS = 10_000L;
    private static final long     ACCOUNT_ID       = 42_002L;
    private static final long     PRICE_FIXED_PT   = 150_0000L;
    private static final long     QTY              = 100L;
    private static final long     SYMBOL_AAPL      = OrderFlyweight.encodeSymbol("AAPL");

    private final PerfConfig              config;
    private final AeronCluster            cluster;
    private final PerfOrderInjector       injector;
    private final PerfExecReportListener  listener;
    private final int                     maxOrders;

    public OrderBookSaturationTest(
            final PerfConfig             config,
            final AeronCluster           cluster,
            final PerfOrderInjector      injector,
            final PerfExecReportListener listener) {
        this.config    = config;
        this.cluster   = cluster;
        this.injector  = injector;
        this.listener  = listener;
        this.maxOrders = intEnv("OMS_MAX_ORDERS", 65_536);
    }

    public BenchmarkResult run() {
        log.info("=== OrderBookSaturationTest: maxOrders={}", maxOrders);

        // Pre-allocate array for injected clOrdIds (need to cancel them afterwards)
        final long[] injectedIds = new long[(int) (maxOrders * 0.91) + LATENCY_SAMPLE + 100];
        int          totalInjected = 0;

        final long[]          p50AtLevel = new long[FILL_LEVELS.length];
        final long[]          p99AtLevel = new long[FILL_LEVELS.length];
        final LatencyHistogram sampleHistogram = new LatencyHistogram();

        int prevTarget = 0;

        for (int lvl = 0; lvl < FILL_LEVELS.length; lvl++) {
            final int target = (int) (maxOrders * FILL_LEVELS[lvl]);
            log.info("  Filling to {} ({}% = {} orders)...",
                    (int)(FILL_LEVELS[lvl]*100), (int)(FILL_LEVELS[lvl]*100), target);

            // Inject orders up to this threshold (no fills expected)
            while (totalInjected < target) {
                final long id = injector.inject(
                        ACCOUNT_ID, SYMBOL_AAPL, Side.BUY, PRICE_FIXED_PT, QTY, false);
                listener.recordSent();
                injectedIds[totalInjected++] = id;
                listener.poll(cluster);
            }

            // Drain outstanding acks before sampling
            drainFor(cluster, listener, 1_000L);

            // Latency sample at this fill level
            sampleHistogram.reset();
            final long prevReceived = listener.totalReceived();
            final long sampleStart  = System.nanoTime();
            long nextSend = sampleStart;
            for (int s = 0; s < LATENCY_SAMPLE; s++) {
                nextSend += config.sendIntervalNs;
                PerfOrderInjector.awaitNextSend(nextSend);
                final long id = injector.inject(
                        ACCOUNT_ID, SYMBOL_AAPL, Side.BUY, PRICE_FIXED_PT, QTY, false);
                listener.recordSent();
                injectedIds[totalInjected++] = id;
                listener.poll(cluster);
            }
            drainFor(cluster, listener, 3_000L);

            p50AtLevel[lvl] = sampleHistogram.percentile(50.0);
            p99AtLevel[lvl] = sampleHistogram.percentile(99.0);
            log.info("  Fill {}%: p50={} µs  p99={} µs",
                    (int)(FILL_LEVELS[lvl]*100), p50AtLevel[lvl]/1_000, p99AtLevel[lvl]/1_000);

            prevTarget = target;
        }

        // ── Drain: cancel all injected orders ────────────────────────────────
        log.info("  Cancelling {} injected orders...", totalInjected);
        final long cancelDeadline = System.currentTimeMillis() + DRAIN_TIMEOUT_MS;
        for (int i = totalInjected - 1; i >= 0; i--) {
            if (injectedIds[i] == 0L) continue;
            injector.injectCancel(ACCOUNT_ID, injectedIds[i], SYMBOL_AAPL);
            listener.recordSent();
            if (i % 100 == 0) listener.poll(cluster);
            if (System.currentTimeMillis() > cancelDeadline) {
                log.warn("  Cancel drain timeout after {} orders", totalInjected - i);
                break;
            }
        }
        drainFor(cluster, listener, 3_000L);

        // ── Assertions ────────────────────────────────────────────────────────
        final double p99Ratio = p99AtLevel[0] > 0
                ? (double) p99AtLevel[FILL_LEVELS.length - 1] / p99AtLevel[0]
                : 1.0;

        final boolean passed = p99Ratio <= 2.0;
        if (!passed) {
            log.warn("  FAIL: p99 at 90% ({} µs) / p99 at 25% ({} µs) = {}x > 2.0x",
                    p99AtLevel[FILL_LEVELS.length - 1] / 1_000,
                    p99AtLevel[0] / 1_000,
                    String.format("%.2f", p99Ratio));
        }

        final Map<String, String> metrics = new LinkedHashMap<>();
        for (int i = 0; i < FILL_LEVELS.length; i++) {
            metrics.put("p99_at_" + (int)(FILL_LEVELS[i]*100) + "pct_us",
                    String.valueOf(p99AtLevel[i] / 1_000));
        }
        metrics.put("p99_ratio_90_vs_25", String.format("%.2f", p99Ratio));

        log.info("  OrderBookSaturationTest: {}", passed ? "PASS" : "FAIL");
        return new BenchmarkResult("OrderBookSaturationTest", passed, metrics);
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
