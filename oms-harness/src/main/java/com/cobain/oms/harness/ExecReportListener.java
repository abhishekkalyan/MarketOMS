package com.cobain.oms.harness;

import com.cobain.oms.codec.ClusterMessageType;
import com.cobain.oms.model.OrderFlyweight;
import com.cobain.oms.model.OrderState;
import com.cobain.oms.model.OrderLayout;
import com.cobain.oms.model.Side;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.EgressListener;
import io.aeron.cluster.codecs.EventCode;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Polls Aeron Cluster egress for exec report responses from OmsNode.
 *
 * Implements EgressListener so it can be wired directly into AeronCluster.Context.
 * Tracks the last observed state for each clOrdId so ScenarioRunner can assert outcomes.
 *
 * Pre-allocates a read flyweight — zero allocation on the polling path.
 */
public final class ExecReportListener implements EgressListener {

    private static final Logger log = LoggerFactory.getLogger(ExecReportListener.class);

    // Last observed state per clOrdId — written by the Aeron egress poller thread.
    // ConcurrentHashMap used here because the harness polls and asserts from the same thread,
    // but Java visibility requires it to be safely published.
    private final ConcurrentHashMap<Long, Byte> lastStateByClOrdId = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Long, Long> filledQtyByClOrdId = new ConcurrentHashMap<>();

    private final AtomicInteger totalReceived = new AtomicInteger(0);
    private final AtomicInteger totalFilled   = new AtomicInteger(0);
    private final AtomicInteger totalCanceled = new AtomicInteger(0);
    private final AtomicInteger totalRejected = new AtomicInteger(0);

    // Pre-allocated read flyweight — reused for every inbound message
    private final OrderFlyweight readFlyweight = new OrderFlyweight();
    private final UnsafeBuffer   readBuffer    = new UnsafeBuffer(
            ByteBuffer.allocateDirect(OrderLayout.BLOCK_LENGTH));

    @Override
    public void onMessage(
            final long clusterSessionId,
            final long timestamp,
            final DirectBuffer buffer,
            final int offset,
            final int length,
            final Header header) {

        if (length < ClusterMessageType.OFFSET_PAYLOAD + OrderLayout.BLOCK_LENGTH) {
            return; // too short to be a valid exec report
        }

        final byte msgType = buffer.getByte(offset + ClusterMessageType.OFFSET_MSG_TYPE);
        if (msgType != ClusterMessageType.NEW_ORDER &&
            msgType != ClusterMessageType.CANCEL_ORDER &&
            msgType != ClusterMessageType.REPLACE_ORDER) {
            return; // not an exec report — ignore (e.g. internal cluster messages)
        }

        // Copy payload into pre-allocated buffer, then wrap flyweight
        buffer.getBytes(offset + ClusterMessageType.OFFSET_PAYLOAD,
                readBuffer, 0, OrderLayout.BLOCK_LENGTH);
        readFlyweight.wrap(readBuffer);

        final long clOrdId    = readFlyweight.clOrdId();
        final byte orderState = readFlyweight.orderState();
        final long filledQty  = readFlyweight.filledQty();
        final long qty        = readFlyweight.qty();
        final String symbol   = OrderFlyweight.decodeSymbol(readFlyweight.symbol());

        lastStateByClOrdId.put(clOrdId, orderState);
        filledQtyByClOrdId.put(clOrdId, filledQty);
        totalReceived.incrementAndGet();

        log.info("EXEC_REPORT clOrdId={} symbol={} side={} state={} filled={}/{} price={}",
                clOrdId,
                symbol,
                Side.nameOf(readFlyweight.side()),
                OrderState.nameOf(orderState),
                filledQty,
                qty,
                readFlyweight.price());

        if (orderState == OrderState.FILLED)   totalFilled.incrementAndGet();
        if (orderState == OrderState.CANCELED) totalCanceled.incrementAndGet();
        if (orderState == OrderState.REJECTED) totalRejected.incrementAndGet();
    }

    @Override
    public void onSessionEvent(
            final long correlationId,
            final long clusterSessionId,
            final long leadershipTermId,
            final int leaderMemberId,
            final EventCode code,
            final String detail) {
        log.info("Cluster session event: code={} detail={}", code, detail);
    }

    @Override
    public void onNewLeader(
            final long clusterSessionId,
            final long leadershipTermId,
            final int leaderMemberId,
            final String ingressEndpoints) {
        log.info("New cluster leader: memberId={}", leaderMemberId);
    }

    // ── Query API used by ScenarioRunner ──────────────────────────────────────

    /** Returns the last observed OrderState byte for this clOrdId, or -1 if not yet seen. */
    public byte lastState(final long clOrdId) {
        final Byte state = lastStateByClOrdId.get(clOrdId);
        return (state != null) ? state : -1;
    }

    /** Returns the last observed filledQty for this clOrdId, or -1 if not yet seen. */
    public long lastFilledQty(final long clOrdId) {
        final Long qty = filledQtyByClOrdId.get(clOrdId);
        return (qty != null) ? qty : -1L;
    }

    public int totalReceived() { return totalReceived.get(); }
    public int totalFilled()   { return totalFilled.get(); }
    public int totalCanceled() { return totalCanceled.get(); }
    public int totalRejected() { return totalRejected.get(); }

    /** Polls the cluster for egress messages. Call from the harness main loop. */
    public int poll(final AeronCluster cluster) {
        return cluster.pollEgress();
    }

    /** Blocks until the given clOrdId reaches the expected state or timeout elapses. */
    public boolean awaitState(
            final AeronCluster cluster,
            final long clOrdId,
            final byte expectedState,
            final long timeoutMs) throws InterruptedException {

        final long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            poll(cluster);
            if (lastState(clOrdId) == expectedState) return true;
            Thread.sleep(5);
        }
        log.warn("Timeout waiting for clOrdId={} to reach state={}; last={}",
                clOrdId, OrderState.nameOf(expectedState), OrderState.nameOf(lastState(clOrdId)));
        return false;
    }
}
