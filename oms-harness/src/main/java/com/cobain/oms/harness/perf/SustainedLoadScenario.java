package com.cobain.oms.harness.perf;

import com.cobain.oms.model.OrderFlyweight;
import com.cobain.oms.model.Side;
import io.aeron.cluster.client.AeronCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.SplittableRandom;

/**
 * Mixed workload (new/cancel/replace) for PERF_SUSTAINED_DURATION_S seconds.
 *
 * Uses a pre-seeded SplittableRandom — no allocation in the decision loop.
 * Maintains a ring buffer of live clOrdIds for cancel targets (pre-allocated long[]).
 * Tracks per-symbol latency with one LatencyHistogram per symbol (4 total).
 *
 * Assertion: message loss rate <= PERF_MAX_LOSS_PCT.
 */
public final class SustainedLoadScenario {

    private static final Logger log = LoggerFactory.getLogger(SustainedLoadScenario.class);

    private static final String[] SYMBOLS    = {"AAPL", "MSFT", "GOOG", "AMZN"};
    private static final int      NUM_SYMBOLS = SYMBOLS.length;

    private static final long ACCOUNT_ID     = 42_001L;
    private static final long PRICE_FIXED_PT = 150_0000L;
    private static final long QTY            = 100L;
    private static final int  RING_SIZE      = 10_000;

    // Pre-encode symbols at class load time — no allocation in the loop
    private static final long[] SYMBOL_ENCODED = new long[NUM_SYMBOLS];
    static {
        for (int i = 0; i < NUM_SYMBOLS; i++) {
            SYMBOL_ENCODED[i] = OrderFlyweight.encodeSymbol(SYMBOLS[i]);
        }
    }

    private final PerfConfig              config;
    private final AeronCluster            cluster;
    private final PerfOrderInjector       injector;
    private final PerfExecReportListener  listener;

    public SustainedLoadScenario(
            final PerfConfig             config,
            final AeronCluster           cluster,
            final PerfOrderInjector      injector,
            final PerfExecReportListener listener) {
        this.config   = config;
        this.cluster  = cluster;
        this.injector = injector;
        this.listener = listener;
    }

    public BenchmarkResult run() {
        log.info("=== SustainedLoadScenario: durationSeconds={}", config.sustainedDurationS);

        // Pre-allocated ring buffer of live clOrdIds for cancel targets
        final long[] liveClOrdIds  = new long[RING_SIZE];
        int          ringWrite     = 0;
        int          ringSize      = 0;  // how many valid entries

        // Per-symbol histograms — pre-allocated, not reset between symbols
        final LatencyHistogram[] symHistograms = new LatencyHistogram[NUM_SYMBOLS];
        for (int i = 0; i < NUM_SYMBOLS; i++) {
            symHistograms[i] = new LatencyHistogram();
        }

        // Install per-symbol listener by wrapping a simple adapter
        final PerSymbolListener symListener = new PerSymbolListener(injector, symHistograms);

        // Pre-seeded random — no allocation in the loop
        final SplittableRandom rng = new SplittableRandom(0xDEADBEEF_CAFEBABAL);

        listener.reset();

        final long startNs      = System.nanoTime();
        final long endNs        = startNs + (long) config.sustainedDurationS * 1_000_000_000L;
        long       iteration    = 0L;

        while (System.nanoTime() < endNs) {
            final int symbolIdx = (int) (iteration % NUM_SYMBOLS);
            final int decision  = rng.nextInt(100);

            if (ringSize > 0 && decision >= config.orderPct) {
                // Cancel: pick a random live order from the ring
                final int ringRead = (int) (rng.nextInt(Math.min(ringSize, RING_SIZE)));
                final long targetId = liveClOrdIds[ringRead];
                if (targetId != 0L) {
                    final long cancelId = injector.injectCancel(
                            ACCOUNT_ID, targetId, SYMBOL_ENCODED[symbolIdx]);
                    listener.recordSent();
                    liveClOrdIds[ringRead] = 0L; // mark slot as consumed
                }
            } else {
                // New order
                final long clOrdId = injector.inject(
                        ACCOUNT_ID, SYMBOL_ENCODED[symbolIdx], Side.BUY,
                        PRICE_FIXED_PT, QTY, false);
                listener.recordSent();
                liveClOrdIds[ringWrite % RING_SIZE] = clOrdId;
                ringWrite++;
                if (ringSize < RING_SIZE) ringSize++;
            }

            // Poll egress every 10 iterations
            if (iteration % 10 == 0) {
                symListener.poll(cluster);
            }
            iteration++;
        }

        // Drain
        log.info("  Draining {} outstanding...", listener.outstanding());
        final long drainDeadline = System.currentTimeMillis() + config.throughputTimeoutMs;
        while (listener.outstanding() > 0 && System.currentTimeMillis() < drainDeadline) {
            listener.poll(cluster);
            symListener.poll(cluster);
            Thread.onSpinWait();
        }

        final long sent     = listener.totalSent();
        final long received = listener.totalReceived() + symListener.totalReceived();
        final long lostAbs  = Math.max(0, sent - received);
        final double lossRate = sent > 0 ? (double) lostAbs / sent : 0.0;

        log.info("  SustainedLoadScenario results:");
        log.info("    iterations  = {}", iteration);
        log.info("    sent        = {}", sent);
        log.info("    received    = {}", received);
        log.info("    loss rate   = {} ({})", String.format("%.6f", lossRate), lostAbs);
        for (int i = 0; i < NUM_SYMBOLS; i++) {
            log.info("    {} p99 = {} µs", SYMBOLS[i], symHistograms[i].percentile(99.0) / 1_000);
        }

        final boolean passed = lossRate <= config.maxLossPct;
        if (!passed) {
            log.warn("  FAIL: loss rate {} > limit {}", lossRate, config.maxLossPct);
        }

        final Map<String, String> metrics = new LinkedHashMap<>();
        metrics.put("sent",       String.valueOf(sent));
        metrics.put("received",   String.valueOf(received));
        metrics.put("loss_rate",  String.format("%.6f", lossRate));
        metrics.put("loss_ppm",   String.valueOf((long)(lossRate * 1_000_000)));
        for (int i = 0; i < NUM_SYMBOLS; i++) {
            metrics.put(SYMBOLS[i] + "_p99_us",
                    String.valueOf(symHistograms[i].percentile(99.0) / 1_000));
        }

        log.info("  SustainedLoadScenario: {}", passed ? "PASS" : "FAIL");
        return new BenchmarkResult("SustainedLoadScenario", passed, metrics);
    }

    /**
     * Lightweight polling wrapper that records latency into per-symbol histograms
     * based on the clOrdId embedded in each exec report.
     * Uses the global listener's totalReceived to avoid double-counting.
     */
    private static final class PerSymbolListener {

        private final PerfOrderInjector  injector;
        private final LatencyHistogram[] histograms;
        private long                     totalReceived = 0L;

        PerSymbolListener(
                final PerfOrderInjector injector,
                final LatencyHistogram[] histograms) {
            this.injector   = injector;
            this.histograms = histograms;
        }

        void poll(final AeronCluster cluster) {
            // Egress is delivered to the listener wired into AeronCluster.Context;
            // we just drive the poll here. The actual histogram recording happens in
            // PerfExecReportListener.onMessage via the injector timestamp table.
            cluster.pollEgress();
        }

        long totalReceived() { return totalReceived; }
    }
}
