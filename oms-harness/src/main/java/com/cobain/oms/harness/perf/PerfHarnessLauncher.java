package com.cobain.oms.harness.perf;

import io.aeron.Aeron;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Entry point for the performance test harness.
 *
 * Lifecycle:
 *   1. Load PerfConfig
 *   2. Start embedded MediaDriver (SHARED threading)
 *   3. Connect AeronCluster
 *   4. Run all six benchmarks in sequence
 *   5. Print consolidated results table
 *   6. Exit 0 if all passed, exit 1 if any failed
 */
public final class PerfHarnessLauncher {

    private static final Logger log = LoggerFactory.getLogger(PerfHarnessLauncher.class);

    public static void main(final String[] args) throws Exception {

        final String aeronDir       = env("HARNESS_AERON_DIR",       "/tmp/oms-aeron-perf");
        final String clusterIngress = env("HARNESS_CLUSTER_INGRESS", "0=localhost:9000");
        final int    maxOrders      = intEnv("OMS_MAX_ORDERS",   65_536);
        final int    maxChildren    = intEnv("OMS_MAX_CHILDREN", 32_768);

        log.info("================================================================");
        log.info("  OMS Performance Harness");
        log.info("  Aeron dir      : {}", aeronDir);
        log.info("  Cluster ingress: {}", clusterIngress);
        log.info("================================================================");

        final PerfConfig config = PerfConfig.load();

        // Clean stale Aeron dir
        deleteDir(aeronDir);
        new java.io.File(aeronDir).mkdirs();

        final MediaDriver mediaDriver = MediaDriver.launch(
                new MediaDriver.Context()
                        .aeronDirectoryName(aeronDir)
                        .threadingMode(ThreadingMode.SHARED)
                        .dirDeleteOnStart(true)
                        .dirDeleteOnShutdown(true));

        log.info("MediaDriver started (Aeron version: {})",
                Aeron.class.getPackage().getImplementationVersion());

        final LatencyHistogram histogram = new LatencyHistogram();

        // We need to wire listener before connecting because AeronCluster.Context requires
        // the EgressListener at construction time. Use a forward-ref placeholder.
        final ForwardRefListener fwd = new ForwardRefListener();

        final AeronCluster cluster = AeronCluster.connect(
                new AeronCluster.Context()
                        .aeronDirectoryName(aeronDir)
                        .ingressChannel("aeron:udp")
                        .ingressEndpoints(clusterIngress)
                        .egressChannel("aeron:udp?endpoint=localhost:0")
                        .egressListener(fwd));

        log.info("Connected to cluster — leaderMemberId={}", cluster.leaderMemberId());

        final PerfOrderInjector      perfInjector = new PerfOrderInjector(cluster);
        final PerfExecReportListener perfListener = new PerfExecReportListener(perfInjector, histogram);
        fwd.setDelegate(perfListener);

        // Shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try { cluster.close(); }    catch (final Exception ignored) {}
            try { mediaDriver.close(); } catch (final Exception ignored) {}
        }));

        // ── Run benchmarks ────────────────────────────────────────────────────
        final List<BenchmarkResult> results = new ArrayList<>();

        results.add(new LatencyBenchmark(config, cluster, perfInjector, perfListener, histogram).run());
        results.add(new ThroughputBenchmark(config, cluster, perfInjector, perfListener).run());
        results.add(new SustainedLoadScenario(config, cluster, perfInjector, perfListener).run());
        results.add(new OrderBookSaturationTest(config, cluster, perfInjector, perfListener).run());
        results.add(new ChildOrderRegistrySaturationTest(config, cluster, perfInjector, perfListener).run());
        results.add(new SnapshotRecoveryBenchmark(config, cluster, perfInjector, perfListener).run());

        cluster.close();
        mediaDriver.close();

        // ── Print consolidated results table ──────────────────────────────────
        printResultsTable(results, maxOrders, maxChildren);

        final long failed = results.stream().filter(r -> !r.passed).count();
        System.exit(failed > 0 ? 1 : 0);
    }

    private static void printResultsTable(
            final List<BenchmarkResult> results,
            final int maxOrders,
            final int maxChildren) {

        final String line = "════════════════════════════════════════════════════════════════";
        final String sep  = "  ─────────────────────────────────────────────────────────────";

        System.out.println(line);
        System.out.println("  PERFORMANCE RESULTS — MarketOMS");
        System.out.println("  Run date         : " + Instant.now());
        System.out.println("  OMS_MAX_ORDERS   : " + maxOrders);
        System.out.println("  OMS_MAX_CHILDREN : " + maxChildren);
        System.out.println(line);
        System.out.printf("  %-30s %-14s %-12s %s%n",
                "Benchmark", "Result", "Threshold", "Status");
        System.out.println(sep);

        for (final BenchmarkResult r : results) {
            final String status = r.passed ? "PASS" : "FAIL";
            // Print primary metric if available
            if (r.metrics.containsKey("p99_us")) {
                row("LatencyBenchmark p99",
                        r.metrics.get("p99_us") + " µs", "1000 µs", r.passed);
            }
            if (r.metrics.containsKey("p99_9_us")) {
                row("LatencyBenchmark p99.9",
                        r.metrics.get("p99_9_us") + " µs", "5000 µs",
                        Long.parseLong(r.metrics.get("p99_9_us")) <= 5000);
            }
            if (r.metrics.containsKey("p99_99_us")) {
                row("LatencyBenchmark p99.99",
                        r.metrics.get("p99_99_us") + " µs", "—", true);
            }
            if (r.metrics.containsKey("gc_collections")) {
                row("LatencyBenchmark GC collections",
                        r.metrics.get("gc_collections"), "0",
                        "0".equals(r.metrics.get("gc_collections")));
            }
            if (r.metrics.containsKey("mean_sent_per_s")) {
                row("ThroughputBenchmark orders/sec",
                        r.metrics.get("mean_sent_per_s"), "—", true);
            }
            if (r.metrics.containsKey("max_outstanding")) {
                row("ThroughputBenchmark outstanding",
                        r.metrics.get("max_outstanding"), "500", r.passed);
            }
            if (r.metrics.containsKey("loss_ppm")) {
                row("SustainedLoad loss rate",
                        r.metrics.get("loss_ppm") + " ppm", "100 ppm", r.passed);
            }
            if (r.metrics.containsKey("p99_ratio_90_vs_25")) {
                row("OrderBookSaturation p99 ratio",
                        r.metrics.get("p99_ratio_90_vs_25") + "×", "2×", r.passed);
            }
            if (r.metrics.containsKey("mean_resume_ms")) {
                row("SnapshotRecovery resume",
                        r.metrics.get("mean_resume_ms") + " ms", "5000 ms", r.passed);
            }
        }

        System.out.println(line);
        final long passed = results.stream().filter(r -> r.passed).count();
        final long failed = results.stream().filter(r -> !r.passed).count();
        System.out.println("  OVERALL: " + (failed == 0 ? "PASS" : "FAIL")
                + "  (" + passed + " passed, " + failed + " failed)");
        System.out.println(line);
    }

    private static void row(
            final String label,
            final String value,
            final String threshold,
            final boolean pass) {
        System.out.printf("  %-30s %-14s %-12s %s%n",
                label, value, threshold, pass ? "PASS" : "FAIL");
    }

    private static String env(final String key, final String defaultValue) {
        final String v = System.getenv(key);
        return (v != null && !v.isEmpty()) ? v : defaultValue;
    }

    private static int intEnv(final String key, final int defaultValue) {
        final String v = System.getenv(key);
        return (v != null && !v.isEmpty()) ? Integer.parseInt(v.trim()) : defaultValue;
    }

    private static void deleteDir(final String path) {
        final java.io.File dir = new java.io.File(path);
        if (!dir.exists()) return;
        final java.io.File[] files = dir.listFiles();
        if (files != null) {
            for (final java.io.File f : files) {
                if (f.isDirectory()) deleteDir(f.getPath());
                else f.delete();
            }
        }
        dir.delete();
    }

    private PerfHarnessLauncher() {}

    /**
     * Forward-reference EgressListener that delegates to PerfExecReportListener once wired.
     * Needed because AeronCluster.Context requires the listener before PerfOrderInjector
     * has a cluster reference.
     */
    private static final class ForwardRefListener implements io.aeron.cluster.client.EgressListener {

        private PerfExecReportListener delegate;

        void setDelegate(final PerfExecReportListener d) {
            this.delegate = d;
        }

        @Override
        public void onMessage(
                final long clusterSessionId,
                final long timestamp,
                final org.agrona.DirectBuffer buffer,
                final int offset,
                final int length,
                final io.aeron.logbuffer.Header header) {
            if (delegate != null) {
                delegate.onMessage(clusterSessionId, timestamp, buffer, offset, length, header);
            }
        }

        @Override
        public void onSessionEvent(
                final long correlationId,
                final long clusterSessionId,
                final long leadershipTermId,
                final int leaderMemberId,
                final io.aeron.cluster.codecs.EventCode code,
                final String detail) {
            if (delegate != null) {
                delegate.onSessionEvent(correlationId, clusterSessionId, leadershipTermId,
                        leaderMemberId, code, detail);
            }
        }

        @Override
        public void onNewLeader(
                final long clusterSessionId,
                final long leadershipTermId,
                final int leaderMemberId,
                final String ingressEndpoints) {
            if (delegate != null) {
                delegate.onNewLeader(clusterSessionId, leadershipTermId, leaderMemberId,
                        ingressEndpoints);
            }
        }
    }
}
