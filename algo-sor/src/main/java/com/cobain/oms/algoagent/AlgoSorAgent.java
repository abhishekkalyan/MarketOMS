package com.cobain.oms.algoagent;

import com.cobain.oms.algo.AlgoExecutionEngine;
import com.cobain.oms.algo.IcebergAlgoEngine;
import com.cobain.oms.algo.SmartOrderRouter;
import com.cobain.oms.algo.TwapAlgoEngine;
import com.cobain.oms.codec.ChildOrderIntentFlyweight;
import com.cobain.oms.codec.ClusterMessageType;
import com.cobain.oms.model.OrderFlyweight;
import com.cobain.oms.model.OrderLayout;
import io.aeron.ExclusivePublication;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Aeron Agent bridging the algo-sor module to the oms-core module via IPC.
 *
 * Receives validated parent orders from oms-core on {@code parentOrderSub}
 * (stream STREAM_PARENT_ORDERS), runs routing computation, and publishes
 * {@link ChildOrderIntentFlyweight} records back on {@code intentPub}
 * (stream STREAM_CHILD_INTENTS).
 *
 * ARCHITECTURAL INVARIANT:
 *   algo-sor publishes ONLY ChildOrderIntent messages. It never sends NOS
 *   messages to the FIX bridge. It never writes to OrderBook or ChildOrderRegistry.
 *
 * ZERO-GC HOT PATH:
 *   - doWork() polls the subscription — no allocation.
 *   - onFragment() wraps the pre-allocated flyweight — no new objects.
 *   - publishIntent() copies 56 bytes into the offer buffer — one memcpy.
 */
public final class AlgoSorAgent implements Agent, FragmentHandler {

    private static final int FRAGMENT_LIMIT = 10;

    // ── Aeron channels ────────────────────────────────────────────────────────
    private final Subscription         parentOrderSub; // stream STREAM_PARENT_ORDERS
    private final ExclusivePublication  intentPub;      // stream STREAM_CHILD_INTENTS

    // ── Algo engines ──────────────────────────────────────────────────────────
    private final IcebergAlgoEngine icebergEngine;
    private final TwapAlgoEngine    twapEngine;
    private final SmartOrderRouter  sor;

    // ── Pre-allocated hot-path state ──────────────────────────────────────────

    /** Re-usable view over the inbound order payload. */
    private final UnsafeBuffer  payloadBuffer = new UnsafeBuffer(new byte[0]);

    /** Flyweight re-pointed at payloadBuffer on every inbound order. */
    private final OrderFlyweight parentView    = new OrderFlyweight();

    /**
     * Offer buffer for outbound CHILD_ORDER_INTENT messages:
     *   [0]   msgType         (byte)
     *   [1]   protocolVersion (byte)
     *   [2-7] padding         (6 bytes)
     *   [8-63] intent payload (ChildOrderIntentFlyweight.BLOCK_LENGTH bytes)
     */
    private final UnsafeBuffer intentOfferBuffer =
            new UnsafeBuffer(new byte[ClusterMessageType.HEADER_LENGTH
                                      + ChildOrderIntentFlyweight.BLOCK_LENGTH]);

    // ── Mock market depth — updated by a market data handler in production ────
    private final int[]  venueIds    = new int[SmartOrderRouter.MAX_VENUES];
    private final long[] venuePrices = new long[SmartOrderRouter.MAX_VENUES];
    private final long[] venueQtys   = new long[SmartOrderRouter.MAX_VENUES];

    public AlgoSorAgent(
            final Subscription         parentOrderSub,
            final ExclusivePublication  intentPub,
            final IcebergAlgoEngine     icebergEngine,
            final TwapAlgoEngine        twapEngine,
            final SmartOrderRouter      sor) {

        this.parentOrderSub  = parentOrderSub;
        this.intentPub       = intentPub;
        this.icebergEngine   = icebergEngine;
        this.twapEngine      = twapEngine;
        this.sor             = sor;

        // Pre-write the header bytes — they never change
        intentOfferBuffer.putByte(ClusterMessageType.OFFSET_MSG_TYPE,
                                  ClusterMessageType.CHILD_ORDER_INTENT);
        intentOfferBuffer.putByte(1, ClusterMessageType.PROTOCOL_VERSION);

        // Pre-load mock market depth: one venue with full qty
        loadDefaultDepth();
    }

    // ── Agent duty cycle ──────────────────────────────────────────────────────

    @Override
    public int doWork() {
        return parentOrderSub.poll(this, FRAGMENT_LIMIT);
    }

    @Override
    public String roleName() {
        return "algo-sor-agent";
    }

    // ── FragmentHandler ───────────────────────────────────────────────────────

    /**
     * Processes one IPC fragment from oms-core.
     * ZERO-GC: no allocation — wraps pre-allocated flyweight, copies bytes.
     */
    @Override
    public void onFragment(
            final DirectBuffer buffer,
            final int offset,
            final int length,
            final Header header) {

        if (length < ClusterMessageType.IPC_MESSAGE_SIZE) {
            return;
        }

        final byte msgType = buffer.getByte(offset + ClusterMessageType.OFFSET_MSG_TYPE);

        if (msgType == ClusterMessageType.NEW_ORDER) {
            // Zero-copy: wrap payloadBuffer as a view over the inbound fragment
            payloadBuffer.wrap(buffer, offset + ClusterMessageType.OFFSET_PAYLOAD,
                               OrderLayout.MESSAGE_SIZE);
            parentView.wrap(payloadBuffer, 0);

            // Run SOR — emits one ChildOrderIntent per venue slice via this::publishIntent
            sor.route(parentView, venueIds, venuePrices, venueQtys,
                      countActiveVenues(), this::publishIntent);

            // For algo orders, call the appropriate engine based on strategy byte (future work):
            // e.g. if (strategyByte == ICEBERG) icebergEngine.onSlice(parentView, this::publishIntent, nowNanos);
            // e.g. if (strategyByte == TWAP)    twapEngine.onSlice(parentView, this::publishIntent, nowNanos);
        }
        // All other message types are ignored by algo-sor
    }

    // ── ChildIntentSink implementation ────────────────────────────────────────

    /**
     * Publish one ChildOrderIntent to oms-core via intentPub.
     *
     * HOT PATH — ZERO ALLOCATIONS:
     *   1. Copy 56 bytes from the engine's pre-alloc'd intent buffer into the offer buffer
     *   2. Offer the 64-byte message to Aeron IPC (spin on back-pressure)
     */
    private void publishIntent(final ChildOrderIntentFlyweight intent) {
        // Copy the 56-byte intent record into the offer buffer at offset HEADER_LENGTH
        intent.copyTo(intentOfferBuffer, ClusterMessageType.HEADER_LENGTH);

        // Offer to oms-core — spin on back-pressure (IPC ring buffer rarely saturates)
        final int totalLength = ClusterMessageType.HEADER_LENGTH
                              + ChildOrderIntentFlyweight.BLOCK_LENGTH;
        while (true) {
            final long result = intentPub.offer(intentOfferBuffer, 0, totalLength);
            if (result > 0L) {
                return;
            }
            if (result == Publication.CLOSED) {
                return;
            }
            Thread.onSpinWait();
        }
    }

    // ── Market depth helpers ──────────────────────────────────────────────────

    private void loadDefaultDepth() {
        // Default: route everything to venue 1 at a benchmark price
        venueIds[0]    = 1;
        venuePrices[0] = 1_000_000L; // £100.0000 in fixed-point x10000
        venueQtys[0]   = Long.MAX_VALUE / 2; // effectively unlimited
        for (int i = 1; i < SmartOrderRouter.MAX_VENUES; i++) {
            venueIds[i] = 0;
            venueQtys[i] = 0L;
        }
    }

    private int countActiveVenues() {
        int count = 0;
        for (int i = 0; i < SmartOrderRouter.MAX_VENUES; i++) {
            if (venueIds[i] > 0 && venueQtys[i] > 0L) {
                count++;
            } else {
                break;
            }
        }
        return Math.max(1, count);
    }
}
