package com.cobain.oms.harness;

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
 * Injects NEW_ORDER messages into a running OmsNode via the Aeron Cluster client API.
 *
 * Wire format sent on each offer():
 *   [0]     msgType = ClusterMessageType.NEW_ORDER (0x01)
 *   [1..128] OrderLayout record (128 bytes)
 *   Total: 129 bytes
 *
 * All buffers are pre-allocated in the constructor — zero allocation on the injection path.
 */
public final class OrderInjector {

    private static final Logger log = LoggerFactory.getLogger(OrderInjector.class);

    private static final int MSG_SIZE = ClusterMessageType.IPC_MESSAGE_SIZE; // 1 + 128 = 129

    private final AeronCluster cluster;

    // Pre-allocated send buffer and flyweight — reused for every order
    private final UnsafeBuffer   sendBuffer;
    private final OrderFlyweight flyweight;

    private long nextClOrdId = 1_000_000L;

    public OrderInjector(final AeronCluster cluster) {
        this.cluster    = cluster;
        this.sendBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(MSG_SIZE));
        this.flyweight  = new OrderFlyweight();

        // Write the fixed message type header once — it never changes
        sendBuffer.putByte(ClusterMessageType.OFFSET_MSG_TYPE, ClusterMessageType.NEW_ORDER);

        // Wrap the flyweight over the payload region (offset 1) of the send buffer
        flyweight.wrap(sendBuffer, ClusterMessageType.OFFSET_PAYLOAD);
    }

    /**
     * Injects a single new order. Returns the clOrdId assigned to this order.
     * Spins on back-pressure using Thread.onSpinWait() — no sleep, no allocation.
     */
    public long inject(
            final long   accountId,
            final String symbol,
            final byte   side,
            final long   priceFixedPt,   // price * OrderLayout.PRICE_MULTIPLIER
            final long   qty,
            final byte   timeInForce) {

        final long clOrdId = nextClOrdId++;

        // Zero-fill the entire payload region before populating (clears reserved bytes)
        sendBuffer.setMemory(ClusterMessageType.OFFSET_PAYLOAD, OrderLayout.BLOCK_LENGTH, (byte) 0);

        // Populate order fields via flyweight (already wrapped at offset 1)
        flyweight.accountId(accountId);
        flyweight.clOrdId(clOrdId);
        flyweight.orderId(0L);                              // assigned by venue on ack
        flyweight.origClOrdId(0L);
        flyweight.symbol(OrderFlyweight.encodeSymbol(symbol));
        flyweight.price(priceFixedPt);
        flyweight.qty(qty);
        flyweight.filledQty(0L);
        flyweight.leavesQty(qty);
        flyweight.side(side);
        flyweight.timeInForce(timeInForce);
        flyweight.orderState(OrderState.PENDING_NEW);
        flyweight.venueId(0);                               // oms-core assigns venue via SOR
        flyweight.transactTime(System.nanoTime());
        flyweight.childCount(0);
        flyweight.nextSiblingSlot(0);
        flyweight.parentOrFirstChildId(0L);

        // Offer to cluster — spin on back-pressure
        while (true) {
            final long result = cluster.offer(sendBuffer, 0, MSG_SIZE);
            if (result > 0L) break;
            if (result == io.aeron.Publication.CLOSED) {
                throw new IllegalStateException("Cluster publication closed");
            }
            Thread.onSpinWait();
        }

        log.debug("Injected NEW_ORDER clOrdId={} symbol={} side={} price={} qty={}",
                clOrdId, symbol, Side.nameOf(side), priceFixedPt, qty);
        return clOrdId;
    }

    /**
     * Injects a CANCEL_ORDER for an existing live order.
     * Uses origClOrdId to reference the original order.
     */
    public void injectCancel(final long accountId, final long origClOrdId, final String symbol) {
        final long clOrdId = nextClOrdId++;

        sendBuffer.putByte(ClusterMessageType.OFFSET_MSG_TYPE, ClusterMessageType.CANCEL_ORDER);
        sendBuffer.setMemory(ClusterMessageType.OFFSET_PAYLOAD, OrderLayout.BLOCK_LENGTH, (byte) 0);

        flyweight.accountId(accountId);
        flyweight.clOrdId(clOrdId);
        flyweight.origClOrdId(origClOrdId);
        flyweight.symbol(OrderFlyweight.encodeSymbol(symbol));
        flyweight.transactTime(System.nanoTime());

        while (true) {
            final long result = cluster.offer(sendBuffer, 0, MSG_SIZE);
            if (result > 0L) break;
            if (result == io.aeron.Publication.CLOSED) {
                throw new IllegalStateException("Cluster publication closed");
            }
            Thread.onSpinWait();
        }

        // Reset type to NEW_ORDER for subsequent inject() calls
        sendBuffer.putByte(ClusterMessageType.OFFSET_MSG_TYPE, ClusterMessageType.NEW_ORDER);

        log.debug("Injected CANCEL_ORDER clOrdId={} origClOrdId={} symbol={}",
                clOrdId, origClOrdId, symbol);
    }
}
