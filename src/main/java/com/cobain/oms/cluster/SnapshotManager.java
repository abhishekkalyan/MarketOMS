package com.cobain.oms.cluster;

import com.cobain.oms.core.OrderBook;
import com.cobain.oms.core.ValidationEngine;
import com.cobain.oms.model.OrderLayout;
import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Snapshot serializer / deserializer for Aeron Cluster failover.
 *
 * ┌─────────────────────────────────────────────────────────────────────────┐
 * │  WHY SNAPSHOTS MATTER FOR ZERO-GC DETERMINISM                          │
 * │                                                                         │
 * │  Without snapshots, a new node joining the cluster must replay every   │
 * │  Raft log entry from the beginning of time to reconstruct the in-memory │
 * │  order book state. Snapshots provide a point-in-time compressed capture │
 * │  of the mutable state (order bytes + dedup map) so the catch-up log    │
 * │  replay starts from the snapshot position, not from position 0.        │
 * │                                                                         │
 * │  During snapshot load the cluster is not yet processing messages, so   │
 * │  minor allocations (FragmentAssembler, lambda) are acceptable here.    │
 * └─────────────────────────────────────────────────────────────────────────┘
 *
 * Snapshot wire format:
 *   [0]  version     : int   (4 bytes)  — schema version for forward compat
 *   [4]  orderCount  : int   (4 bytes)  — number of serialized order records
 *   [8]  dedupCount  : int   (4 bytes)  — number of dedup map entries
 *  [12]  reserved    : int   (4 bytes)
 *  [16..16+orderCount*80]  order records (ORDER_RECORD_SIZE each)
 *  [16+orderCount*80 .. +dedupCount*16]  dedup entries (clOrdId:8 + orderId:8)
 */
public final class SnapshotManager {

    public static final int SNAPSHOT_VERSION   = 1;
    public static final int HEADER_SIZE        = 16;
    public static final int DEDUP_ENTRY_SIZE   = 16; // clOrdId (8) + orderId (8)

    // ── Pre-allocated snapshot serialization buffer ────────────────────────────
    //
    // Worst-case snapshot size:
    //   HEADER (16) + MAX_ORDERS * MESSAGE_SIZE (65536 * 80 = 5,242,880)
    //   + MAX_ORDERS * DEDUP_ENTRY_SIZE (65536 * 16 = 1,048,576)
    //   ≈ 6.3 MB — sized conservatively to avoid runtime allocation.
    //
    private static final int MAX_SNAPSHOT_BYTES =
            HEADER_SIZE
            + OrderBook.MAX_ORDERS * OrderLayout.MESSAGE_SIZE
            + OrderBook.MAX_ORDERS * DEDUP_ENTRY_SIZE;

    private final UnsafeBuffer snapshotBuffer =
            new UnsafeBuffer(java.nio.ByteBuffer.allocateDirect(MAX_SNAPSHOT_BYTES));

    // ── Fragment handler for snapshot loading (allocated once at construction) ──
    private OrderBook        restoreTargetBook;
    private ValidationEngine restoreTargetValidation;
    private final FragmentHandler snapshotFragmentHandler = this::handleSnapshotFragment;

    // ── State for multi-fragment snapshot loading ──────────────────────────────
    private int  loadedOrders;
    private int  loadedDedupEntries;
    private int  expectedOrders;
    private int  expectedDedupEntries;
    private boolean headerLoaded;

    // ── Serialization ─────────────────────────────────────────────────────────

    /**
     * Serialize the full OMS state into the Aeron snapshot publication.
     * Called by {@code OmsClusteredService.onTakeSnapshot()}.
     *
     * Iteration uses Agrona's zero-boxing forEach; the total write is one contiguous
     * buffer offer, minimising the number of Aeron fragments.
     *
     * @param idleStrategy passed by the Cluster; must be called during back-pressure
     */
    public void takeSnapshot(
            final OrderBook orderBook,
            final ValidationEngine validation,
            final ExclusivePublication snapshotPublication,
            final IdleStrategy idleStrategy) {

        int position = 0;

        // ── Build the snapshot into the pre-allocated buffer ──────────────────

        // Header: version, order count, dedup count, reserved
        final int orderCount = orderBook.openOrderCount();
        final int dedupCount = validation.seenClOrdIds().size();

        snapshotBuffer.putInt(0,  SNAPSHOT_VERSION);
        snapshotBuffer.putInt(4,  orderCount);
        snapshotBuffer.putInt(8,  dedupCount);
        snapshotBuffer.putInt(12, 0); // reserved
        position = HEADER_SIZE;

        // ── Orders: bulk copy each slot's 80 bytes ────────────────────────────
        final int[] orderPosition = {position}; // effectively-final wrapper for lambda
        orderBook.forEachActiveSlot(slot -> {
            snapshotBuffer.putBytes(
                    orderPosition[0],
                    orderBook.buffer(),
                    orderBook.offsetForSlot(slot),
                    OrderLayout.MESSAGE_SIZE);
            orderPosition[0] += OrderLayout.MESSAGE_SIZE;
        });
        position = orderPosition[0];

        // ── Dedup map: clOrdId + orderId per entry ────────────────────────────
        final int[] dedupPosition = {position};
        validation.seenClOrdIds().forEach((clOrdId, orderId) -> {
            snapshotBuffer.putLong(dedupPosition[0],     clOrdId);
            snapshotBuffer.putLong(dedupPosition[0] + 8, orderId);
            dedupPosition[0] += DEDUP_ENTRY_SIZE;
        });
        position = dedupPosition[0];

        // ── Offer to Aeron (may require multiple offers under back-pressure) ──
        long result;
        do {
            result = snapshotPublication.offer(snapshotBuffer, 0, position);
            idleStrategy.idle();
        } while (result < 0);
    }

    // ── Deserialization ───────────────────────────────────────────────────────

    /**
     * Restore OMS state from a snapshot image.
     * Called by {@code OmsClusteredService.onStart()} when snapshotImage is non-null.
     *
     * Reads fragment-by-fragment using Aeron's image poll loop until end-of-stream.
     */
    public void loadSnapshot(
            final Image snapshotImage,
            final OrderBook orderBook,
            final ValidationEngine validation,
            final IdleStrategy idleStrategy) {

        // Clear all state before restore
        orderBook.reset();
        validation.reset();

        // Configure restore targets (accessible in the fragment handler)
        this.restoreTargetBook       = orderBook;
        this.restoreTargetValidation = validation;
        this.headerLoaded            = false;
        this.loadedOrders            = 0;
        this.loadedDedupEntries      = 0;
        this.expectedOrders          = 0;
        this.expectedDedupEntries    = 0;

        // Poll until the snapshot image stream is exhausted
        while (!snapshotImage.isEndOfStream()) {
            final int fragments = snapshotImage.poll(snapshotFragmentHandler, 10);
            idleStrategy.idle(fragments);
        }
    }

    // ── Fragment handler (called by Aeron during snapshot load) ───────────────

    private void handleSnapshotFragment(
            final DirectBuffer buffer,
            final int offset,
            final int length,
            final Header header) {

        int pos = offset;

        // First fragment: read the header
        if (!headerLoaded) {
            if (length < HEADER_SIZE) {
                throw new IllegalStateException("Snapshot header too short: " + length);
            }
            final int version = buffer.getInt(pos);
            if (version != SNAPSHOT_VERSION) {
                throw new IllegalStateException("Unsupported snapshot version: " + version);
            }
            expectedOrders       = buffer.getInt(pos + 4);
            expectedDedupEntries = buffer.getInt(pos + 8);
            pos += HEADER_SIZE;
            headerLoaded = true;
        }

        // Restore orders
        while (loadedOrders < expectedOrders
               && pos + OrderLayout.MESSAGE_SIZE <= offset + length) {
            restoreTargetBook.restoreOrder(buffer, pos);
            pos += OrderLayout.MESSAGE_SIZE;
            loadedOrders++;
        }

        // Restore dedup entries
        while (loadedDedupEntries < expectedDedupEntries
               && pos + DEDUP_ENTRY_SIZE <= offset + length) {
            final long clOrdId = buffer.getLong(pos);
            final long orderId = buffer.getLong(pos + 8);
            restoreTargetValidation.restoreEntry(clOrdId, orderId);
            pos += DEDUP_ENTRY_SIZE;
            loadedDedupEntries++;
        }
    }
}
