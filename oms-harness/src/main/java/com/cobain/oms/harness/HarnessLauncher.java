package com.cobain.oms.harness;

import io.aeron.Aeron;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Entry point for the test harness.
 *
 * Lifecycle:
 *   1. Embed a local Aeron MediaDriver (separate dir from OmsNode)
 *   2. Connect to the OmsNode cluster as a client
 *   3. Run all ScenarioRunner scenarios
 *   4. Print pass/fail summary
 *   5. Exit 0 on full pass, exit 1 on any failure
 *
 * Configuration (environment variables):
 *   HARNESS_AERON_DIR       — Aeron dir for this harness process (default /tmp/oms-aeron-harness)
 *   HARNESS_CLUSTER_INGRESS — cluster ingress endpoint (default localhost:9000)
 *   HARNESS_TIMEOUT_MS      — per-scenario timeout in ms (default 8000)
 */
public final class HarnessLauncher {

    private static final Logger log = LoggerFactory.getLogger(HarnessLauncher.class);

    public static void main(final String[] args) throws Exception {

        final String aeronDir       = env("HARNESS_AERON_DIR",       "/tmp/oms-aeron-harness");
        final String clusterIngress = env("HARNESS_CLUSTER_INGRESS",  "localhost:9000");

        log.info("================================================================");
        log.info("  OMS Test Harness");
        log.info("  Aeron dir      : {}", aeronDir);
        log.info("  Cluster ingress: {}", clusterIngress);
        log.info("================================================================");

        // Clean stale harness Aeron dir
        deleteDir(aeronDir);
        new java.io.File(aeronDir).mkdirs();

        // ── 1. Embedded MediaDriver for the harness process ───────────────────
        final MediaDriver mediaDriver = MediaDriver.launch(
                new MediaDriver.Context()
                        .aeronDirectoryName(aeronDir)
                        .threadingMode(ThreadingMode.SHARED)
                        .dirDeleteOnStart(true)
                        .dirDeleteOnShutdown(true));

        log.info("Harness MediaDriver started (Aeron version: {})",
                Aeron.class.getPackage().getImplementationVersion());

        // ── 2. Egress listener (pre-wired before cluster connect) ─────────────
        final ExecReportListener listener = new ExecReportListener();

        // ── 3. Connect to OmsNode cluster ─────────────────────────────────────
        log.info("Connecting to OmsNode cluster at {}...", clusterIngress);
        final AeronCluster cluster = AeronCluster.connect(
                new AeronCluster.Context()
                        .aeronDirectoryName(aeronDir)
                        .ingressChannel("aeron:udp")
                        .ingressEndpoints(clusterIngress)
                        .egressChannel("aeron:udp?endpoint=localhost:0")
                        .egressListener(listener));

        log.info("Connected to cluster — leaderMemberId={}", cluster.leaderMemberId());

        // ── 4. Wire up injector and scenario runner ───────────────────────────
        final OrderInjector   injector = new OrderInjector(cluster);
        final ScenarioRunner  runner   = new ScenarioRunner(cluster, injector, listener);

        // ── 5. Run all scenarios ──────────────────────────────────────────────
        try {
            runner.scenarioSingleOrderAccepted();
            runner.scenarioRejectedInvalidQty();
            runner.scenarioRejectedUnknownSymbol();
            runner.scenarioCancelRequest();
            runner.scenarioBulkInjection();
        } finally {
            runner.printSummary();

            cluster.close();
            mediaDriver.close();
        }

        System.exit(runner.allPassed() ? 0 : 1);
    }

    private static String env(final String key, final String defaultValue) {
        final String v = System.getenv(key);
        return (v != null && !v.isEmpty()) ? v : defaultValue;
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

    private HarnessLauncher() {}
}
