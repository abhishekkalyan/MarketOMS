package com.cobain.oms.harness.perf;

import com.cobain.oms.model.OrderFlyweight;
import com.cobain.oms.model.Side;
import io.aeron.cluster.client.AeronCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Measures maximum sustained throughput (orders/sec) without rate limiting.
 *
 * Injects in a tight loop for PERF_DURATION_SECONDS seconds, polling egress every
 * N=10 sends. Pauses injection when outstanding > PERF_MAX_OUTSTANDING until it drains.
 * Reports every 5 seconds and at run end.
 */
public final class ThroughputBenchmark {

    private static final Logger log = LoggerFactory.getLogger(ThroughputBenchmark.class);

    private static final int    POLL_EVERY           = 1;
    private static final long   REPORT_INTERVAL_MS   = 5_000L;
    private static final long   ACCOUNT_ID           = 42_001L;
    private static final long   PRICE_FIXED_PT       = 150_0000L;
    private static final long   QTY                  = 100L;
    private static final long   SYMBOL_AAPL          = OrderFlyweight.encodeSymbol("AAPL");

    private final PerfConfig              config;
    private final AeronCluster            cluster;
    private final PerfOrderInjector       injector;
    private final PerfExecReportListener  listener;

    public ThroughputBenchmark(
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
        log.info("=== ThroughputBenchmark starting: durationSeconds={}", config.durationSeconds);

        listener.reset();

        final long startNs          = System.nanoTime();
        final long endNs            = startNs + (long) config.durationSeconds * 1_000_000_000L;
        long       nextReportMs     = System.currentTimeMillis() + REPORT_INTERVAL_MS;
        long       intervalSentBase = 0L;
        long       maxOutstanding   = 0L;

        int  sendCount = 0;

        while (System.nanoTime() < endNs) {
            // Back-pressure gate: drain until outstanding drops below threshold (1s max)
            if (listener.outstanding() >= config.maxOutstanding) {
                final long bpDeadline = System.currentTimeMillis() + 1_000L;
                while (listener.outstanding() >= config.maxOutstanding
                        && System.currentTimeMillis() < bpDeadline) {
                    listener.poll(cluster);
                    Thread.onSpinWait();
                }
            }

            injector.inject(ACCOUNT_ID, SYMBOL_AAPL, Side.BUY, PRICE_FIXED_PT, QTY, false);
            listener.recordSent();
            sendCount++;

            if (sendCount % POLL_EVERY == 0) {
                listener.poll(cluster);
                final long out = listener.outstanding();
                if (out > maxOutstanding) maxOutstanding = out;
            }

            // Periodic progress report
            final long nowMs = System.currentTimeMillis();
            if (nowMs >= nextReportMs) {
                final long totalSent = listener.totalSent();
                final long totalRecv = listener.totalReceived();
                final long elapsedS  = (System.nanoTime() - startNs) / 1_000_000_000L;
                final long sentSec   = elapsedS > 0 ? totalSent / elapsedS : totalSent;
                log.info("  [+{}s] sent={} recv={} outstanding={} sent/s={}",
                        elapsedS, totalSent, totalRecv, listener.outstanding(), sentSec);
                nextReportMs += REPORT_INTERVAL_MS;
                intervalSentBase = totalSent;
            }
        }

        // Final drain
        log.info("  Injection done. Draining {} outstanding...", listener.outstanding());
        final long drainDeadline = System.currentTimeMillis() + config.throughputTimeoutMs;
        while (listener.outstanding() > 0 && System.currentTimeMillis() < drainDeadline) {
            listener.poll(cluster);
            Thread.onSpinWait();
        }

        final long totalSent    = listener.totalSent();
        final long totalReceived = listener.totalReceived();
        final long elapsedS     = (long) config.durationSeconds;
        final long meanSentSec  = elapsedS > 0 ? totalSent / elapsedS : totalSent;

        log.info("  ThroughputBenchmark results:");
        log.info("    total sent     = {}", totalSent);
        log.info("    total received = {}", totalReceived);
        log.info("    mean sent/sec  = {}", meanSentSec);
        log.info("    max outstanding= {}", maxOutstanding);

        // Assertions
        boolean passed = true;
        if (maxOutstanding > config.maxOutstanding) {
            log.warn("  FAIL: max outstanding {} > limit {}", maxOutstanding, config.maxOutstanding);
            passed = false;
        }
        final long minRequired = (long) (totalSent * 0.90);
        if (totalReceived < minRequired) {
            log.warn("  FAIL: received {} < 90% of sent {} = {}", totalReceived, totalSent, minRequired);
            passed = false;
        }

        final Map<String, String> metrics = new LinkedHashMap<>();
        metrics.put("total_sent",      String.valueOf(totalSent));
        metrics.put("total_received",  String.valueOf(totalReceived));
        metrics.put("mean_sent_per_s", String.valueOf(meanSentSec));
        metrics.put("max_outstanding", String.valueOf(maxOutstanding));

        log.info("  ThroughputBenchmark: {}", passed ? "PASS" : "FAIL");
        return new BenchmarkResult("ThroughputBenchmark", passed, metrics);
    }
}
