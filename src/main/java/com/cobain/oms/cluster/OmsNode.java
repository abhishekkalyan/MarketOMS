package com.cobain.oms.cluster;

import com.cobain.oms.model.OrderFlyweight;
import com.cobain.oms.transport.AeronTransport;
import io.aeron.Aeron;
import io.aeron.ExclusivePublication;
import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.client.AeronArchive;
import io.aeron.cluster.ClusteredMediaDriver;
import io.aeron.cluster.ConsensusModule;
import io.aeron.cluster.service.ClusteredServiceContainer;
import io.aeron.driver.MediaDriver;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OMS Node entry point.
 *
 * Starts a fully embedded Aeron Cluster node comprising:
 *   1. MediaDriver          — zero-copy transport (IPC + UDP)
 *   2. Archive              — persistent Raft log for snapshot + replay
 *   3. ConsensusModule      — Raft leader election and log commitment
 *   4. ClusteredServiceContainer — hosts OmsClusteredService
 *
 * ┌─────────────────────────────────────────────────────────────────────────┐
 * │  ACTIVE-PASSIVE CLUSTER TOPOLOGY                                        │
 * │                                                                         │
 * │  Run 3 instances of OmsNode (nodeId 0, 1, 2) on separate hosts.        │
 * │  Configure clusterMembers to include all three host:port pairs.        │
 * │  Aeron Cluster elects one leader (active) via Raft.                    │
 * │  The two followers (passive) receive every committed log entry and     │
 * │  replay it through OmsClusteredService identically, keeping their      │
 * │  in-memory state current.                                               │
 * │                                                                         │
 * │  On leader failure: remaining nodes hold a Raft election in < 1 second │
 * │  (configurable heartbeat/election timeouts), the new leader begins     │
 * │  accepting client sessions, and order processing resumes transparently. │
 * └─────────────────────────────────────────────────────────────────────────┘
 *
 * Configuration (environment variables or system properties):
 *   OMS_NODE_ID          — integer 0/1/2 identifying this node
 *   OMS_CLUSTER_MEMBERS  — comma-separated host:port triples for all nodes
 *                          e.g. "0,localhost:9000:9001:9002:9003|1,host2:9000:9001:9002:9003"
 *   OMS_AERON_DIR        — path for Aeron shared memory files
 *   OMS_ARCHIVE_DIR      — path for Aeron Archive files (persistent log)
 *   OMS_MAX_NOTIONAL     — per-order notional limit in base currency units
 *   OMS_SYMBOLS          — comma-separated ASCII symbols e.g. "AAPL,MSFT,GOOG"
 */
public final class OmsNode {

    private static final Logger log = LoggerFactory.getLogger(OmsNode.class);

    public static void main(final String[] args) throws Exception {

        // ── Configuration ──────────────────────────────────────────────────────
        final int    nodeId          = intEnv("OMS_NODE_ID", 0);
        final String aeronDir        = env("OMS_AERON_DIR", "/dev/shm/oms-aeron-" + nodeId);
        final String archiveDir      = env("OMS_ARCHIVE_DIR", "/tmp/oms-archive-" + nodeId);
        final long   maxNotional     = longEnv("OMS_MAX_NOTIONAL", 10_000_000L);
        final String symbolsEnv      = env("OMS_SYMBOLS", "AAPL,MSFT,GOOG,AMZN");
        final String clusterMembers  = env("OMS_CLUSTER_MEMBERS",
                "0,localhost:9000:9001:9002:0:9003|"
                + "1,localhost:9010:9011:9012:0:9013|"
                + "2,localhost:9020:9021:9022:0:9023");

        // Pre-encode all permitted symbols into longs at startup (off hot path)
        final long[] permittedSymbols = parseSymbols(symbolsEnv);

        log.info("Starting OMS node {} with {} permitted symbols", nodeId, permittedSymbols.length);

        // ── Aeron MediaDriver ──────────────────────────────────────────────────
        final MediaDriver.Context mediaDriverCtx = buildMediaDriverContext(aeronDir);

        // ── Aeron Archive ──────────────────────────────────────────────────────
        final Archive.Context archiveCtx = new Archive.Context()
                .aeronDirectoryName(aeronDir)
                .archiveDirectoryName(archiveDir)
                .threadingMode(ArchiveThreadingMode.SHARED)
                .deleteArchiveOnStart(false); // retain archive for snapshot recovery

        // ── ConsensusModule (Raft) ─────────────────────────────────────────────
        // In Aeron 1.40+, clusterMemberId(int) identifies this node within the cluster members
        // string (replaces the older memberId / clusterMembersStatusEndpoints API).
        final ConsensusModule.Context consensusCtx = new ConsensusModule.Context()
                .aeronDirectoryName(aeronDir)
                .archiveContext(new AeronArchive.Context().aeronDirectoryName(aeronDir))
                .clusterMembers(clusterMembers)
                .clusterMemberId(nodeId)
                .clusterDir(new java.io.File(archiveDir + "/cluster"));

        // ── OMS Service — Aeron publications are created after MediaDriver is up ─
        // We launch the MediaDriver first, then connect publications.
        // Using try-with-resources: ClusteredMediaDriver closes everything in order.
        try (ClusteredMediaDriver clusteredMediaDriver =
                     ClusteredMediaDriver.launch(mediaDriverCtx, archiveCtx, consensusCtx)) {

            // After the MediaDriver is running, create an Aeron client for publications
            try (Aeron aeron = AeronTransport.connectAeron(aeronDir)) {

                // One ExclusivePublication per venue FIX engine (IPC)
                final ExclusivePublication[] venuePublications = new ExclusivePublication[]{
                        AeronTransport.createIpcPublication(aeron, AeronTransport.STREAM_VENUE_1_OUT),
                        AeronTransport.createIpcPublication(aeron, AeronTransport.STREAM_VENUE_2_OUT),
                        AeronTransport.createIpcPublication(aeron, AeronTransport.STREAM_VENUE_3_OUT),
                };

                // Publication back to the client-facing FIX engine
                final ExclusivePublication clientPublication =
                        AeronTransport.createIpcPublication(aeron, AeronTransport.STREAM_OMS_TO_FIX);

                // Instantiate the OMS clustered service
                final OmsClusteredService omsService = new OmsClusteredService(
                        venuePublications,
                        clientPublication,
                        maxNotional,
                        permittedSymbols);

                // ── ClusteredServiceContainer ───────────────────────────────────
                final ClusteredServiceContainer.Context serviceCtx =
                        new ClusteredServiceContainer.Context()
                                .aeronDirectoryName(aeronDir)
                                .archiveContext(
                                        new AeronArchive.Context().aeronDirectoryName(aeronDir))
                                .clusterDir(new java.io.File(archiveDir + "/cluster"))
                                .clusteredService(omsService);

                try (ClusteredServiceContainer container =
                             ClusteredServiceContainer.launch(serviceCtx)) {

                    log.info("OMS node {} is running. Waiting for shutdown signal...", nodeId);

                    // Block until SIGTERM / SIGINT — Agrona's barrier handles both cleanly
                    new ShutdownSignalBarrier().await();

                    log.info("OMS node {} shutting down", nodeId);
                }
            }
        }
    }

    // ── Configuration helpers ─────────────────────────────────────────────────

    private static MediaDriver.Context buildMediaDriverContext(final String aeronDir) {
        return new MediaDriver.Context()
                .aeronDirectoryName(aeronDir)
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true)
                // DEDICATED: separate OS threads for conductor, sender, receiver.
                // Pin each to a CPU core for deterministic latency (done externally via
                // numactl/taskset; thread names are "aeron-conductor", "aeron-sender", etc.)
                .threadingMode(io.aeron.driver.ThreadingMode.DEDICATED)
                .conductorIdleStrategy(new org.agrona.concurrent.BusySpinIdleStrategy())
                .senderIdleStrategy(new org.agrona.concurrent.BusySpinIdleStrategy())
                .receiverIdleStrategy(new org.agrona.concurrent.BusySpinIdleStrategy())
                // Pre-fault all term buffer pages at startup to eliminate
                // page-fault jitter on the first write to each buffer page.
                .termBufferSparseFile(false)
                .performStorageChecks(false);
    }

    /**
     * Encode ASCII ticker symbols into primitive longs at startup.
     * This is the only time we iterate a String array — pure off-path work.
     */
    private static long[] parseSymbols(final String symbolsEnv) {
        final String[] parts = symbolsEnv.split(",");
        final long[] longs = new long[parts.length];
        for (int i = 0; i < parts.length; i++) {
            longs[i] = OrderFlyweight.encodeSymbol(parts[i].trim());
        }
        return longs;
    }

    /** Extract status endpoint string required by ConsensusModule. */
    private static String extractStatusEndpoints(final String clusterMembers) {
        // Format expected by Aeron Cluster: "host1:port1,host2:port2,host3:port3"
        // The clusterMembers string contains these embedded; for now return as-is.
        // In production parse the pipe-separated entries and extract the status port.
        return "localhost:9000,localhost:9010,localhost:9020";
    }

    private static String env(final String key, final String defaultValue) {
        final String v = System.getenv(key);
        return (v != null && !v.isEmpty()) ? v : defaultValue;
    }

    private static int intEnv(final String key, final int defaultValue) {
        final String v = System.getenv(key);
        return (v != null && !v.isEmpty()) ? Integer.parseInt(v) : defaultValue;
    }

    private static long longEnv(final String key, final long defaultValue) {
        final String v = System.getenv(key);
        return (v != null && !v.isEmpty()) ? Long.parseLong(v) : defaultValue;
    }

    private OmsNode() {}
}
