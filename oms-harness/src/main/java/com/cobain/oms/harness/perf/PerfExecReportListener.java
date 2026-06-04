package com.cobain.oms.harness.perf;

import com.cobain.oms.codec.ClusterMessageType;
import com.cobain.oms.model.OrderFlyweight;
import com.cobain.oms.model.OrderLayout;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.EgressListener;
import io.aeron.cluster.codecs.EventCode;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Performance-aware egress listener that records round-trip latency into a LatencyHistogram.
 *
 * <h2>Zero-GC contract</h2>
 * {@link #onMessage} is zero-allocation: no new, no boxing, no String ops.
 * GC snapshot methods allocate once (iterate over a stable bean list).
 */
public final class PerfExecReportListener implements EgressListener {

    private static final Logger log = LoggerFactory.getLogger(PerfExecReportListener.class);

    private final PerfOrderInjector  injector;
    private final LatencyHistogram   histogram;
    private final UnsafeBuffer       readBuffer;
    private final OrderFlyweight     readFlyweight;

    private final AtomicLong totalSent     = new AtomicLong(0L);
    private final AtomicLong totalReceived = new AtomicLong(0L);

    // GC snapshot state — written only during snapshotGcBefore/After, not on hot path
    private long gcCountBefore  = 0L;
    private long gcTimeMsBefore = 0L;
    private long gcCountAfter   = 0L;
    private long gcTimeMsAfter  = 0L;

    private final List<GarbageCollectorMXBean> gcBeans;

    public PerfExecReportListener(
            final PerfOrderInjector injector,
            final LatencyHistogram  histogram) {
        this.injector      = injector;
        this.histogram     = histogram;
        this.readBuffer    = new UnsafeBuffer(ByteBuffer.allocateDirect(OrderLayout.BLOCK_LENGTH));
        this.readFlyweight = new OrderFlyweight();
        this.gcBeans       = ManagementFactory.getGarbageCollectorMXBeans();
    }

    @Override
    public void onMessage(
            final long clusterSessionId,
            final long timestamp,
            final DirectBuffer buffer,
            final int offset,
            final int length,
            final Header header) {

        if (length < ClusterMessageType.OFFSET_PAYLOAD + OrderLayout.BLOCK_LENGTH) {
            return;
        }

        buffer.getBytes(offset + ClusterMessageType.OFFSET_PAYLOAD,
                readBuffer, 0, OrderLayout.BLOCK_LENGTH);
        readFlyweight.wrap(readBuffer);

        final long clOrdId   = readFlyweight.clOrdId();
        final long sendTime  = injector.getSendTimestamp(clOrdId);
        if (sendTime != 0L) {
            final long latencyNs = System.nanoTime() - sendTime;
            histogram.record(latencyNs);
        }
        totalReceived.incrementAndGet();
    }

    @Override
    public void onSessionEvent(
            final long correlationId,
            final long clusterSessionId,
            final long leadershipTermId,
            final int leaderMemberId,
            final EventCode code,
            final String detail) {
        if (code == EventCode.CLOSED || code == EventCode.ERROR) {
            log.warn("Cluster session event: code={} detail={} — session may be closed", code, detail);
        } else {
            log.debug("Cluster session event: code={}", code);
        }
    }

    @Override
    public void onNewLeader(
            final long clusterSessionId,
            final long leadershipTermId,
            final int leaderMemberId,
            final String ingressEndpoints) {
        log.debug("New cluster leader: memberId={}", leaderMemberId);
    }

    /** Snapshot GC counters before the measurement run. */
    public void snapshotGcBefore() {
        long count = 0L;
        long time  = 0L;
        for (int i = 0; i < gcBeans.size(); i++) {
            final GarbageCollectorMXBean bean = gcBeans.get(i);
            final long c = bean.getCollectionCount();
            final long t = bean.getCollectionTime();
            if (c >= 0) count += c;
            if (t >= 0) time  += t;
        }
        gcCountBefore  = count;
        gcTimeMsBefore = time;
    }

    /** Snapshot GC counters after the measurement run. */
    public void snapshotGcAfter() {
        long count = 0L;
        long time  = 0L;
        for (int i = 0; i < gcBeans.size(); i++) {
            final GarbageCollectorMXBean bean = gcBeans.get(i);
            final long c = bean.getCollectionCount();
            final long t = bean.getCollectionTime();
            if (c >= 0) count += c;
            if (t >= 0) time  += t;
        }
        gcCountAfter  = count;
        gcTimeMsAfter = time;
    }

    /** Returns the number of GC collections that occurred between snapshots. */
    public long gcCollectionsDelta() {
        return gcCountAfter - gcCountBefore;
    }

    /** Returns cumulative GC pause time (ms) that occurred between snapshots. */
    public long gcTimeDeltaMs() {
        return gcTimeMsAfter - gcTimeMsBefore;
    }

    /** Increments the sent counter. Call once per injected order. */
    public void recordSent() {
        totalSent.incrementAndGet();
    }

    public long totalSent()     { return totalSent.get(); }
    public long totalReceived() { return totalReceived.get(); }
    public long outstanding()   { return totalSent.get() - totalReceived.get(); }

    /** Polls the cluster egress. Call from the benchmark polling loop. */
    public int poll(final AeronCluster cluster) {
        return cluster.pollEgress();
    }

    /** Resets counters and histogram for a fresh benchmark run. */
    public void reset() {
        totalSent.set(0L);
        totalReceived.set(0L);
        histogram.reset();
    }
}
