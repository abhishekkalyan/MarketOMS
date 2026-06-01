package com.cobain.oms.transport;

import io.aeron.Aeron;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.ExclusivePublication;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;

/**
 * Factory and configuration helpers for Aeron channels.
 *
 * ┌─────────────────────────────────────────────────────────────────────────┐
 * │  CHANNEL STRATEGY                                                       │
 * │                                                                         │
 * │  IPC (intra-process shared memory):                                    │
 * │    Used between the OMS ClusteredServiceContainer and the FIX          │
 * │    connectivity engines when they live in the same OS process.         │
 * │    Latency: ~100–300 ns. No kernel, no socket, no copy.                │
 * │                                                                         │
 * │  UDP Unicast:                                                           │
 * │    Used for Aeron Cluster inter-node communication (Raft log).         │
 * │    Also used when the FIX engine is a separate OS process.             │
 * │                                                                         │
 * │  ExclusivePublication:                                                  │
 * │    Always preferred over Publication when there is only one writer     │
 * │    per channel/stream — avoids the CAS overhead of concurrent writers. │
 * └─────────────────────────────────────────────────────────────────────────┘
 */
public final class AeronTransport {

    // ── Well-known stream IDs — must match across OMS and FIX engine configs ──
    public static final int STREAM_OMS_TO_FIX    = 10; // OMS → FIX connectivity (outbound)
    public static final int STREAM_FIX_TO_OMS    = 11; // FIX connectivity → OMS (inbound)
    public static final int STREAM_VENUE_1_OUT   = 20; // OMS → venue 1 FIX engine
    public static final int STREAM_VENUE_2_OUT   = 21; // OMS → venue 2 FIX engine
    public static final int STREAM_VENUE_3_OUT   = 22; // OMS → venue 3 FIX engine

    // ── IPC streams connecting oms-core ↔ algo-sor ───────────────────────────
    public static final int STREAM_OMS_TO_ALGO   = 30; // oms-core → algo-sor: parent orders
    public static final int STREAM_CHILD_INTENTS = 12; // algo-sor → oms-core: ChildOrderIntent messages

    // ── IPC channel URI (no parameters needed for IPC) ────────────────────────
    public static final String IPC_CHANNEL = "aeron:ipc";

    /**
     * Constructs a UDP unicast channel URI.
     * @param host target host (e.g. "192.168.1.10")
     * @param port target port
     */
    public static String udpChannel(final String host, final int port) {
        return new ChannelUriStringBuilder()
                .media("udp")
                .endpoint(host + ":" + port)
                .build();
    }

    /**
     * Creates an embedded MediaDriver configured for low-latency OMS operation.
     *
     * Key settings:
     *   SHARED threading model: spins the conductor, sender, and receiver on dedicated
     *   threads (use DEDICATED for absolute lowest latency when CPU cores are available).
     *   termBufferSparseFile=false: pre-fault term buffers to avoid page faults on the
     *   hot path (mechanical sympathy: touch pages at startup, not at message time).
     */
    public static MediaDriver launchEmbeddedMediaDriver(final String aeronDirectory) {
        final MediaDriver.Context ctx = new MediaDriver.Context()
                .aeronDirectoryName(aeronDirectory)
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true)
                .threadingMode(ThreadingMode.DEDICATED)  // dedicated threads per role
                .conductorIdleStrategy(
                        new org.agrona.concurrent.BusySpinIdleStrategy())  // zero-sleep spinning
                .senderIdleStrategy(
                        new org.agrona.concurrent.BusySpinIdleStrategy())
                .receiverIdleStrategy(
                        new org.agrona.concurrent.BusySpinIdleStrategy())
                .termBufferSparseFile(false)             // pre-fault all term buffer pages
                .performStorageChecks(false);            // skip at startup for speed

        return MediaDriver.launch(ctx);
    }

    /**
     * Creates an Aeron client connected to the given MediaDriver directory.
     */
    public static Aeron connectAeron(final String aeronDirectory) {
        return Aeron.connect(new Aeron.Context().aeronDirectoryName(aeronDirectory));
    }

    /**
     * Creates an ExclusivePublication on the IPC channel for the given stream.
     * ExclusivePublication has no CAS synchronisation — safe when there is exactly
     * one writer thread (the OMS execution loop).
     */
    public static ExclusivePublication createIpcPublication(
            final Aeron aeron,
            final int streamId) {
        return aeron.addExclusivePublication(IPC_CHANNEL, streamId);
    }

    /**
     * Creates a Subscription on the IPC channel for the given stream.
     */
    public static Subscription createIpcSubscription(
            final Aeron aeron,
            final int streamId) {
        return aeron.addSubscription(IPC_CHANNEL, streamId);
    }

    /**
     * Creates a UDP ExclusivePublication (for cross-process / cross-host routing).
     */
    public static ExclusivePublication createUdpPublication(
            final Aeron aeron,
            final String host,
            final int port,
            final int streamId) {
        return aeron.addExclusivePublication(udpChannel(host, port), streamId);
    }

    private AeronTransport() {}
}
