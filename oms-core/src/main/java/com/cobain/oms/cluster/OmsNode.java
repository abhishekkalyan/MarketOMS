package com.cobain.oms.cluster;

import com.cobain.oms.config.ChainedConfigSource;
import com.cobain.oms.config.ConfigSource;
import com.cobain.oms.config.EnvVarConfigSource;
import com.cobain.oms.config.OmsConfig;
import com.cobain.oms.config.PropertiesFileConfigSource;
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
        // Infrastructure-specific values (Aeron dirs, cluster topology) stay here
        // because oms-config deliberately has no Aeron dependency.
        final int    nodeId     = intEnv("OMS_NODE_ID", 0);
        final String aeronDir   = env("OMS_AERON_DIR", "/dev/shm/oms-aeron-" + nodeId);
        final String archiveDir = env("OMS_ARCHIVE_DIR",
                System.getProperty("user.home") + "/oms-archive-" + nodeId);

        // Capacity and tuning values — resolved via OmsConfig (supports env vars
        // and optional properties file pointed to by OMS_CONFIG_FILE).
        final OmsConfig omsConfig = loadOmsConfig();
        final int    maxOrders           = omsConfig.maxOrders;
        final int    maxChildren         = omsConfig.maxChildren;
        final int    intentFragmentLimit = omsConfig.intentFragmentLimit;
        final long   maxNotional         = omsConfig.maxNotional;
        final String symbolsEnv          = omsConfig.symbols;
        // Format (Aeron 1.40+): <id>,<clientHost:port>,<memberHost:port>,<logHost:port>,<transferHost:port>,<archiveHost:port>
        // Separate multiple members with '|'. Port layout per node (base + offset):
        //   clientPort=9000, memberPort=9001, logPort=9002, transferPort=9003, archiveControlPort=8010
        // For multi-node replace localhost with each node's host/IP.
        final String clusterMembers  = env("OMS_CLUSTER_MEMBERS",
                "0,localhost:9000,localhost:9001,localhost:9002,localhost:9003,localhost:8010");
        // Local default: OS-assigned port — fine for single-node dev.
        // Multi-node: set to aeron:udp?endpoint=<this-node-ip>:8020 (fixed port, per node).
        final String replicationChannel = env("OMS_ARCHIVE_REPLICATION_CHANNEL",
                "aeron:udp?endpoint=localhost:0");
        // Archive bind channel — used by the Archive itself and for cross-node replication.
        final String archiveControlChannel = env("OMS_ARCHIVE_CONTROL_CHANNEL",
                "aeron:udp?endpoint=localhost:8010");
        // Local client channels — ConsensusModule and ClusteredServiceContainer talk to the
        // co-located archive over IPC. Must be aeron:ipc when archive is embedded in-process.
        // Only change to UDP if running a standalone (out-of-process) archive.
        final String archiveLocalControlChannel = env("OMS_ARCHIVE_LOCAL_CONTROL_CHANNEL",
                "aeron:ipc");
        final String archiveLocalResponseChannel = env("OMS_ARCHIVE_LOCAL_RESPONSE_CHANNEL",
                "aeron:ipc");
        // Ingress channel: where the Raft leader listens for client session requests.
        // Port must match the client-port slot in OMS_CLUSTER_MEMBERS for this node.
        final String ingressChannel = env("OMS_CLUSTER_INGRESS_CHANNEL",
                "aeron:udp?endpoint=localhost:9000");

        // Pre-encode all permitted symbols into longs at startup (off hot path)
        final long[] permittedSymbols = parseSymbols(symbolsEnv);

        // OMS_ARCHIVE_DELETE_ON_START=true wipes all archive state on each restart.
        // Set to false (default) for production so snapshots survive restarts.
        // Set to true for dev/test harness to guarantee a clean state each run.
        final boolean deleteArchiveOnStart = boolEnv("OMS_ARCHIVE_DELETE_ON_START", false);

        log.info("Starting OMS node {} with {} permitted symbols (deleteArchiveOnStart={})",
                 nodeId, permittedSymbols.length, deleteArchiveOnStart);

        // ── Aeron MediaDriver ──────────────────────────────────────────────────
        final MediaDriver.Context mediaDriverCtx = buildMediaDriverContext(aeronDir);

        final Archive.Context archiveCtx = new Archive.Context()
                .aeronDirectoryName(aeronDir)
                .archiveDirectoryName(archiveDir)
                .controlChannel(archiveControlChannel)
                .replicationChannel(replicationChannel)
                .threadingMode(ArchiveThreadingMode.SHARED)
                .deleteArchiveOnStart(deleteArchiveOnStart);

        // ── ConsensusModule (Raft) ─────────────────────────────────────────────
        // In Aeron 1.40+, clusterMemberId(int) identifies this node within the cluster members
        // string (replaces the older memberId / clusterMembersStatusEndpoints API).
        final java.io.File clusterDir = new java.io.File(archiveDir + "/cluster");
        // Startup canvass timeout: how long the ConsensusModule waits for other members
        // before declaring itself leader. Default is 60s which is too long for dev/test.
        // Set OMS_STARTUP_CANVASS_TIMEOUT_MS to reduce (default 5000ms = 5 seconds for single-node dev).
        final long startupCanvassTimeoutMs = longEnv("OMS_STARTUP_CANVASS_TIMEOUT_MS", 5_000L);
        final ConsensusModule.Context consensusCtx = new ConsensusModule.Context()
                .aeronDirectoryName(aeronDir)
                .archiveContext(new AeronArchive.Context()
                        .aeronDirectoryName(aeronDir)
                        .controlRequestChannel(archiveLocalControlChannel)
                        .controlResponseChannel(archiveLocalResponseChannel))
                .ingressChannel(ingressChannel)
                .replicationChannel(replicationChannel)
                .clusterMembers(clusterMembers)
                .clusterMemberId(nodeId)
                .startupCanvassTimeoutNs(java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(startupCanvassTimeoutMs))
                .deleteDirOnStart(deleteArchiveOnStart)
                .clusterDir(clusterDir);

        // ── OMS Service — Aeron publications are created after MediaDriver is up ─
        // We launch the MediaDriver first, then connect publications.
        // Using try-with-resources: ClusteredMediaDriver closes everything in order.
        try (ClusteredMediaDriver clusteredMediaDriver =
                     ClusteredMediaDriver.launch(mediaDriverCtx, archiveCtx, consensusCtx)) {

            // After the MediaDriver is running, create an Aeron client for publications
            try (Aeron aeron = AeronTransport.connectAeron(aeronDir)) {

                // IPC publication to algo-sor (parent orders for routing)
                final ExclusivePublication algoSorPublication =
                        AeronTransport.createIpcPublication(aeron, AeronTransport.STREAM_OMS_TO_ALGO);

                // Publication back to the client-facing FIX engine
                final ExclusivePublication clientPublication =
                        AeronTransport.createIpcPublication(aeron, AeronTransport.STREAM_OMS_TO_FIX);

                // Subscription for ChildOrderIntent messages from algo-sor (stream 12).
                // Image handlers detect algo-sor connect/disconnect for operational alerting.
                final io.aeron.Subscription intentSub = aeron.addSubscription(
                        AeronTransport.IPC_CHANNEL,
                        AeronTransport.STREAM_CHILD_INTENTS,
                        image -> log.info("algo-sor connected: sessionId={} position={}",
                                image.sessionId(), image.position()),
                        image -> log.warn("algo-sor disconnected: sessionId={} position={}",
                                image.sessionId(), image.position()));

                // Instantiate the OMS clustered service
                final OmsClusteredService omsService = new OmsClusteredService(
                        algoSorPublication,
                        clientPublication,
                        intentSub,
                        maxNotional,
                        maxOrders,
                        maxChildren,
                        intentFragmentLimit,
                        permittedSymbols);

                // ── ClusteredServiceContainer ───────────────────────────────────
                final ClusteredServiceContainer.Context serviceCtx =
                        new ClusteredServiceContainer.Context()
                                .aeronDirectoryName(aeronDir)
                                .archiveContext(
                                        new AeronArchive.Context()
                                                .aeronDirectoryName(aeronDir)
                                                .controlRequestChannel(archiveLocalControlChannel)
                                                .controlResponseChannel(archiveLocalResponseChannel))
                                .clusterDir(clusterDir)
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

    /**
     * Build the {@link OmsConfig} for this node.
     *
     * If {@code OMS_CONFIG_FILE} is set, the file source takes priority over env
     * vars (so a properties file can override anything). When the env var is absent,
     * only the env-var source is used.
     */
    private static OmsConfig loadOmsConfig() {
        final String filePath = System.getenv(OmsConfig.KEY_CONFIG_FILE);
        final ConfigSource source;
        if (filePath != null && !filePath.isEmpty()) {
            source = new ChainedConfigSource(
                    new PropertiesFileConfigSource(filePath),
                    EnvVarConfigSource.INSTANCE);
        } else {
            source = EnvVarConfigSource.INSTANCE;
        }
        return OmsConfig.load(source);
    }

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

    private static boolean boolEnv(final String key, final boolean defaultValue) {
        final String v = System.getenv(key);
        return (v != null && !v.isEmpty()) ? Boolean.parseBoolean(v) : defaultValue;
    }

    private static long longEnv(final String key, final long defaultValue) {
        final String v = System.getenv(key);
        return (v != null && !v.isEmpty()) ? Long.parseLong(v) : defaultValue;
    }

    private OmsNode() {}
}
