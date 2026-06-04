package com.cobain.oms.harness.perf;

import com.cobain.oms.model.OrderFlyweight;
import com.cobain.oms.model.Side;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.ClusterControl;
import io.aeron.cluster.ClusterControl.ToggleState;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Measures time from snapshot trigger to first probe order processed after the snapshot.
 *
 * Protocol (repeated 3 times):
 *   1. Fill OrderBook to 50% capacity with probe orders
 *   2. Trigger snapshot via ClusterControl toggle on the cluster mark file
 *   3. Continuously inject probe orders; measure time until first exec report arrives
 *   4. Report min/max/mean resume latency
 *
 * Snapshot trigger uses ClusterControl.findControlToggle() + ToggleState.SNAPSHOT.toggle().
 * The cluster dir is read from HARNESS_CLUSTER_DIR (default /tmp/oms-archive-0/cluster).
 *
 * Assertion: mean resume latency <= PERF_SNAPSHOT_RESUME_LIMIT_MS.
 */
public final class SnapshotRecoveryBenchmark {

    private static final Logger log = LoggerFactory.getLogger(SnapshotRecoveryBenchmark.class);

    private static final int    FILL_TO_50_PCT_FACTOR = 2; // fill to 50% = maxOrders/2
    private static final int    PROBE_BATCH           = 10;
    private static final long   PROBE_TIMEOUT_MS      = 15_000L;
    private static final int    REPEAT_COUNT          = 3;
    private static final long   ACCOUNT_ID            = 42_004L;
    private static final long   PRICE_FIXED_PT        = 150_0000L;
    private static final long   QTY                   = 100L;
    private static final long   SYMBOL_AAPL           = OrderFlyweight.encodeSymbol("AAPL");

    private final PerfConfig              config;
    private final AeronCluster            cluster;
    private final PerfOrderInjector       injector;
    private final PerfExecReportListener  listener;
    private final int                     maxOrders;
    /** Path to the OmsNode Aeron directory — used to map cluster counters. */
    private final File                    omsAeronDir;

    public SnapshotRecoveryBenchmark(
            final PerfConfig             config,
            final AeronCluster           cluster,
            final PerfOrderInjector      injector,
            final PerfExecReportListener listener) {
        this.config      = config;
        this.cluster     = cluster;
        this.injector    = injector;
        this.listener    = listener;
        this.maxOrders   = intEnv("OMS_MAX_ORDERS", 65_536);
        this.omsAeronDir = new File(env("OMS_AERON_DIR", "/tmp/oms-aeron-0"));
    }

    public BenchmarkResult run() {
        log.info("=== SnapshotRecoveryBenchmark: repeat={} omsAeronDir={}",
                REPEAT_COUNT, omsAeronDir);

        final long[] resumeLatencies = new long[REPEAT_COUNT];

        // ── Phase 1: fill OrderBook to 50% ────────────────────────────────────
        log.info("  Filling OrderBook to 50% ({} orders)...", maxOrders / FILL_TO_50_PCT_FACTOR);
        listener.reset();
        final int fillTarget = maxOrders / FILL_TO_50_PCT_FACTOR;
        final long[] fillIds = new long[fillTarget];
        for (int i = 0; i < fillTarget; i++) {
            fillIds[i] = injector.inject(
                    ACCOUNT_ID, SYMBOL_AAPL, Side.BUY, PRICE_FIXED_PT, QTY, false);
            listener.recordSent();
            if (i % 100 == 0) listener.poll(cluster);
        }
        drainFor(cluster, listener, 2_000L);

        for (int rep = 0; rep < REPEAT_COUNT; rep++) {
            log.info("  Snapshot run {}/{}...", rep + 1, REPEAT_COUNT);

            // ── Phase 2: trigger snapshot ─────────────────────────────────────
            final long triggerTimeNs = System.nanoTime();
            boolean triggered = triggerSnapshot();
            if (!triggered) {
                log.warn("  Could not trigger snapshot — skipping run {}", rep + 1);
                resumeLatencies[rep] = -1L;
                continue;
            }
            log.info("  Snapshot triggered at T+0");

            // ── Phase 3: probe until response arrives ─────────────────────────
            listener.reset();
            final long probeDeadline = System.currentTimeMillis() + PROBE_TIMEOUT_MS;
            long resumeLatencyMs = -1L;

            while (System.currentTimeMillis() < probeDeadline) {
                for (int p = 0; p < PROBE_BATCH; p++) {
                    injector.inject(ACCOUNT_ID, SYMBOL_AAPL, Side.BUY, PRICE_FIXED_PT, QTY, false);
                    listener.recordSent();
                }
                listener.poll(cluster);

                if (listener.totalReceived() > 0) {
                    resumeLatencyMs = (System.nanoTime() - triggerTimeNs) / 1_000_000L;
                    break;
                }
                Thread.onSpinWait();
            }

            if (resumeLatencyMs < 0) {
                log.warn("  No response received within {}ms — snapshot may not have completed",
                        PROBE_TIMEOUT_MS);
                resumeLatencyMs = PROBE_TIMEOUT_MS;
            }

            resumeLatencies[rep] = resumeLatencyMs;
            log.info("  Resume latency: {} ms", resumeLatencyMs);

            // Wait a bit before next run to let cluster stabilise
            drainFor(cluster, listener, 1_000L);
        }

        // ── Compute stats ─────────────────────────────────────────────────────
        long minMs = Long.MAX_VALUE;
        long maxMs = Long.MIN_VALUE;
        long sumMs = 0L;
        int  valid = 0;
        for (final long l : resumeLatencies) {
            if (l >= 0) {
                if (l < minMs) minMs = l;
                if (l > maxMs) maxMs = l;
                sumMs += l;
                valid++;
            }
        }
        final long meanMs = valid > 0 ? sumMs / valid : PROBE_TIMEOUT_MS;
        if (minMs == Long.MAX_VALUE) minMs = PROBE_TIMEOUT_MS;
        if (maxMs == Long.MIN_VALUE) maxMs = PROBE_TIMEOUT_MS;

        log.info("  SnapshotRecoveryBenchmark results: min={}ms max={}ms mean={}ms",
                minMs, maxMs, meanMs);

        final boolean passed = meanMs <= config.snapshotResumeLimitMs;
        if (!passed) {
            log.warn("  FAIL: mean resume {}ms > limit {}ms", meanMs, config.snapshotResumeLimitMs);
        }

        final Map<String, String> metrics = new LinkedHashMap<>();
        metrics.put("min_resume_ms",  String.valueOf(minMs));
        metrics.put("max_resume_ms",  String.valueOf(maxMs));
        metrics.put("mean_resume_ms", String.valueOf(meanMs));

        log.info("  SnapshotRecoveryBenchmark: {}", passed ? "PASS" : "FAIL");
        return new BenchmarkResult("SnapshotRecoveryBenchmark", passed, metrics);
    }

    private boolean triggerSnapshot() {
        try {
            // Map the OmsNode Aeron counters and find the cluster control toggle
            final CountersReader counters = ClusterControl.mapCounters(omsAeronDir);
            final AtomicCounter  toggle   = ClusterControl.findControlToggle(counters,
                    intEnv("OMS_NODE_ID", 0));
            if (toggle == null) {
                log.warn("  ClusterControl toggle not found in {}", omsAeronDir);
                return false;
            }
            final boolean applied = ToggleState.SNAPSHOT.toggle(toggle);
            if (applied) {
                log.info("  ToggleState.SNAPSHOT toggled on OmsNode cluster control");
            } else {
                log.warn("  ToggleState.SNAPSHOT toggle returned false (cluster busy?)");
            }
            return applied;
        } catch (final Exception e) {
            log.warn("  Failed to trigger snapshot via ClusterControl: {}", e.getMessage());
            return false;
        }
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

    private static String env(final String key, final String defaultValue) {
        final String v = System.getenv(key);
        return (v != null && !v.isEmpty()) ? v : defaultValue;
    }
}
