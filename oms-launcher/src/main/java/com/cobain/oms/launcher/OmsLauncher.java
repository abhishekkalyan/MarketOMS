package com.cobain.oms.launcher;

import com.cobain.oms.algo.IcebergAlgoEngine;
import com.cobain.oms.algo.SmartOrderRouter;
import com.cobain.oms.algo.TwapAlgoEngine;
import com.cobain.oms.algoagent.AlgoSorAgent;
import com.cobain.oms.codec.ClusterMessageType;
import com.cobain.oms.config.ChainedConfigSource;
import com.cobain.oms.config.ConfigSource;
import com.cobain.oms.config.EnvVarConfigSource;
import com.cobain.oms.config.OmsConfig;
import com.cobain.oms.config.PropertiesFileConfigSource;
import com.cobain.oms.core.ParentOrderState;
import com.cobain.oms.transport.AeronTransport;
import io.aeron.Aeron;
import io.aeron.ExclusivePublication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Process entry point — wires all modules and starts the AgentRunner for algo-sor.
 *
 * Stream map:
 *   Stream 30 (STREAM_OMS_TO_ALGO):   oms-core → algo-sor  (validated parent orders)
 *   Stream 12 (STREAM_CHILD_INTENTS): algo-sor → oms-core  (ChildOrderIntent messages)
 *
 * Stream 31 (old STREAM_ALGO_TO_OMS) is removed: algo-sor no longer dispatches
 * child NOS to the FIX bridge directly. All child creation happens in oms-core.
 */
public final class OmsLauncher {

    private static final Logger log = LoggerFactory.getLogger(OmsLauncher.class);

    public static void main(final String[] args) throws Exception {

        final String aeronDir = env("OMS_AERON_DIR", "/dev/shm/oms-aeron-launcher");

        // Capacity and tuning — resolved via OmsConfig (env vars + optional properties file).
        final OmsConfig omsConfig       = loadOmsConfig();
        final int       algoFragmentLimit = omsConfig.algoFragmentLimit;
        final int       maxVenues         = omsConfig.maxVenues;

        log.info("Starting OmsLauncher — Aeron version: {}",
                 io.aeron.Aeron.class.getPackage().getImplementationVersion());

        // Register ROUTING state transitions BEFORE any AgentRunners are started
        ParentOrderState.registerTransitions();
        log.info("ParentOrderState transitions registered (ROUTING={})", ParentOrderState.ROUTING);

        // ── 1. Embedded MediaDriver — low-latency, DEDICATED threads ─────────
        final MediaDriver mediaDriver = MediaDriver.launch(
                new MediaDriver.Context()
                        .aeronDirectoryName(aeronDir)
                        .threadingMode(ThreadingMode.DEDICATED)
                        .conductorIdleStrategy(new BusySpinIdleStrategy())
                        .senderIdleStrategy(new BusySpinIdleStrategy())
                        .receiverIdleStrategy(new BusySpinIdleStrategy())
                        .dirDeleteOnStart(true)
                        .dirDeleteOnShutdown(true)
                        .termBufferSparseFile(false)
                        .performStorageChecks(false));

        // ── 2. Aeron client ────────────────────────────────────────────────────
        final Aeron aeron = Aeron.connect(
                new Aeron.Context().aeronDirectoryName(aeronDir));

        // ── 3. IPC channels ────────────────────────────────────────────────────
        // Stream 30: oms-core publishes parent orders → algo-sor subscribes
        final Subscription parentOrderSub =
                AeronTransport.createIpcSubscription(aeron, AeronTransport.STREAM_OMS_TO_ALGO);

        // Stream 12: algo-sor publishes ChildOrderIntents → oms-core subscribes
        final ExclusivePublication intentPub =
                AeronTransport.createIpcPublication(aeron, ClusterMessageType.STREAM_CHILD_INTENTS);

        // ── 4. Pre-allocate algo engines and SOR ───────────────────────────────
        final SmartOrderRouter  sor           = new SmartOrderRouter(maxVenues);
        final IcebergAlgoEngine icebergEngine = new IcebergAlgoEngine(10L);
        final TwapAlgoEngine    twapEngine    = new TwapAlgoEngine();

        // ── 5. AlgoSorAgent + AgentRunner ─────────────────────────────────────
        final AlgoSorAgent algoSorAgent = new AlgoSorAgent(
                parentOrderSub,
                intentPub,
                icebergEngine,
                twapEngine,
                sor,
                algoFragmentLimit,
                maxVenues);

        final AgentRunner agentRunner = new AgentRunner(
                new BusySpinIdleStrategy(),
                Throwable::printStackTrace,
                null,
                algoSorAgent);

        // ── 6. Start on a dedicated thread ────────────────────────────────────
        AgentRunner.startOnThread(agentRunner);
        log.info("AlgoSorAgent started on dedicated thread — publishing intents to stream {}",
                 ClusterMessageType.STREAM_CHILD_INTENTS);

        // ── 7. JVM shutdown hook — close in reverse order ─────────────────────
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down OmsLauncher...");
            agentRunner.close();
            aeron.close();
            mediaDriver.close();
            log.info("OmsLauncher shutdown complete");
        }, "oms-shutdown"));

        // Block until the AgentRunner thread exits (on error or interrupt)
        agentRunner.thread().join();
    }

    /**
     * Build the {@link OmsConfig} for the launcher process.
     *
     * If {@code OMS_CONFIG_FILE} is set, the file source takes priority over env vars.
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

    private static String env(final String key, final String defaultValue) {
        final String v = System.getenv(key);
        return (v != null && !v.isEmpty()) ? v : defaultValue;
    }

    private OmsLauncher() {}
}
