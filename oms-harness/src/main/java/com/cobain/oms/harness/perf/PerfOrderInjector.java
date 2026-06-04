package com.cobain.oms.harness.perf;

import com.cobain.oms.codec.ClusterMessageType;
import com.cobain.oms.model.OrderFlyweight;
import com.cobain.oms.model.OrderLayout;
import com.cobain.oms.model.OrderState;
import com.cobain.oms.model.Side;
import com.cobain.oms.model.TimeInForce;
import io.aeron.cluster.client.AeronCluster;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * High-performance order injector that embeds send timestamps and rate-controls injection.
 *
 * <h2>clOrdId ranges</h2>
 * <ul>
 *   <li>1_000_000 – 9_999_999 : correctness harness (OrderInjector)</li>
 *   <li>10_000_000 – 19_999_999 : warmup orders (discarded)</li>
 *   <li>20_000_000+              : measurement orders</li>
 * </ul>
 * The distinct ranges ensure ValidationEngine.seenClOrdIds dedup does not collide.
 *
 * <h2>Zero-GC contract</h2>
 * {@link #inject} and {@link #awaitNextSend} are zero-allocation: all buffers are
 * pre-allocated in the constructor.
 */
public final class PerfOrderInjector {

    private static final Logger log = LoggerFactory.getLogger(PerfOrderInjector.class);

    private static final int  MSG_SIZE          = ClusterMessageType.IPC_MESSAGE_SIZE;
    private static final long WARMUP_START_ID   = 10_000_000L;
    private static final long MEASURE_START_ID  = 20_000_000L;

    /** Lookup table size; must be a power of 2 for fast modulo via bitmask. */
    private static final int TIMESTAMP_TABLE_SIZE = 1 << 20; // 1 048 576
    private static final int TIMESTAMP_TABLE_MASK = TIMESTAMP_TABLE_SIZE - 1;

    private final AeronCluster   cluster;
    private final UnsafeBuffer   sendBuffer;
    private final OrderFlyweight flyweight;

    /** Pre-allocated send-timestamp lookup table indexed by (clOrdId & MASK). */
    private final long[] sendTimestamps;

    private long nextWarmupId   = WARMUP_START_ID;
    private long nextMeasureId  = MEASURE_START_ID;

    public PerfOrderInjector(final AeronCluster cluster) {
        this.cluster        = cluster;
        this.sendBuffer     = new UnsafeBuffer(ByteBuffer.allocateDirect(MSG_SIZE));
        this.flyweight      = new OrderFlyweight();
        this.sendTimestamps = new long[TIMESTAMP_TABLE_SIZE];

        sendBuffer.putByte(ClusterMessageType.OFFSET_MSG_TYPE, ClusterMessageType.NEW_ORDER);
        flyweight.wrap(sendBuffer, ClusterMessageType.OFFSET_PAYLOAD);
    }

    /**
     * Injects a performance-measurement NEW_ORDER. Embeds send timestamp at RESERVED1_OFFSET.
     * Returns the clOrdId assigned.
     *
     * @param warmup true = use warmup clOrdId range (timestamps not recorded)
     */
    public long inject(
            final long   accountId,
            final long   symbolEncoded,
            final byte   side,
            final long   priceFixedPt,
            final long   qty,
            final boolean warmup) {

        final long clOrdId = warmup ? nextWarmupId++ : nextMeasureId++;

        sendBuffer.setMemory(ClusterMessageType.OFFSET_PAYLOAD, OrderLayout.BLOCK_LENGTH, (byte) 0);
        sendBuffer.putByte(ClusterMessageType.OFFSET_MSG_TYPE, ClusterMessageType.NEW_ORDER);

        flyweight.accountId(accountId);
        flyweight.clOrdId(clOrdId);
        flyweight.orderId(0L);
        flyweight.origClOrdId(0L);
        flyweight.symbol(symbolEncoded);
        flyweight.price(priceFixedPt);
        flyweight.qty(qty);
        flyweight.filledQty(0L);
        flyweight.leavesQty(qty);
        flyweight.side(side);
        flyweight.timeInForce(TimeInForce.DAY);
        flyweight.orderState(OrderState.PENDING_NEW);
        flyweight.venueId(0);
        flyweight.childCount(0);
        flyweight.nextSiblingSlot(0);
        flyweight.parentOrFirstChildId(0L);

        final long sendTime = System.nanoTime();
        flyweight.transactTime(sendTime);
        // Embed timestamp in RESERVED1_OFFSET for round-trip measurement
        sendBuffer.putLong(ClusterMessageType.OFFSET_PAYLOAD + OrderLayout.RESERVED1_OFFSET, sendTime);

        if (!warmup) {
            sendTimestamps[(int) (clOrdId & TIMESTAMP_TABLE_MASK)] = sendTime;
        }

        int spinCount = 0;
        while (true) {
            final long result = cluster.offer(sendBuffer, 0, MSG_SIZE);
            if (result > 0L) break;
            if (result == io.aeron.Publication.CLOSED) {
                return clOrdId; // session closed — return gracefully
            }
            // Poll egress every 64 spins to drain back-pressure
            if ((++spinCount & 63) == 0) {
                cluster.pollEgress();
            }
            Thread.onSpinWait();
        }

        return clOrdId;
    }

    /**
     * Injects a CANCEL_ORDER for an existing live order. Returns the new clOrdId.
     * Uses measurement clOrdId range.
     */
    public long injectCancel(
            final long accountId,
            final long origClOrdId,
            final long symbolEncoded) {

        final long clOrdId = nextMeasureId++;

        sendBuffer.putByte(ClusterMessageType.OFFSET_MSG_TYPE, ClusterMessageType.CANCEL_ORDER);
        sendBuffer.setMemory(ClusterMessageType.OFFSET_PAYLOAD, OrderLayout.BLOCK_LENGTH, (byte) 0);

        flyweight.accountId(accountId);
        flyweight.clOrdId(clOrdId);
        flyweight.origClOrdId(origClOrdId);
        flyweight.symbol(symbolEncoded);

        final long sendTime = System.nanoTime();
        flyweight.transactTime(sendTime);
        sendBuffer.putLong(ClusterMessageType.OFFSET_PAYLOAD + OrderLayout.RESERVED1_OFFSET, sendTime);
        sendTimestamps[(int) (clOrdId & TIMESTAMP_TABLE_MASK)] = sendTime;

        int spinCount = 0;
        while (true) {
            final long result = cluster.offer(sendBuffer, 0, MSG_SIZE);
            if (result > 0L) break;
            if (result == io.aeron.Publication.CLOSED) {
                return clOrdId;
            }
            if ((++spinCount & 63) == 0) {
                cluster.pollEgress();
            }
            Thread.onSpinWait();
        }

        // Reset to NEW_ORDER type for subsequent inject() calls
        sendBuffer.putByte(ClusterMessageType.OFFSET_MSG_TYPE, ClusterMessageType.NEW_ORDER);
        return clOrdId;
    }

    /**
     * Returns the recorded send timestamp for a measurement clOrdId.
     * ZERO allocation. Returns 0 if not found (warmup or evicted by table wrap).
     */
    public long getSendTimestamp(final long clOrdId) {
        return sendTimestamps[(int) (clOrdId & TIMESTAMP_TABLE_MASK)];
    }

    /**
     * Busy-spins until at least {@code intervalNs} has elapsed since the last call.
     * ZERO allocation. Uses System.nanoTime() — never Thread.sleep().
     *
     * @param nextSendNs nanosecond timestamp of the next scheduled send
     * @return updated next send time (= nextSendNs + intervalNs)
     */
    public static long awaitNextSend(final long nextSendNs) {
        while (System.nanoTime() < nextSendNs) {
            Thread.onSpinWait();
        }
        return nextSendNs;
    }

    /** Returns the next measurement clOrdId that will be assigned (without consuming it). */
    public long peekNextMeasureId() {
        return nextMeasureId;
    }
}
