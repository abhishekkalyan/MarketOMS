package com.cobain.oms.cluster;

import com.cobain.oms.core.ChildOrderRegistry;
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
 * Snapshot wire format (version 3):
 *   [0-3]   version           : int  — schema version
 *   [4-7]   orderCount        : int  — number of serialized parent order records
 *   [8-11]  dedupCount        : int  — number of dedup map entries
 *  [12-15]  reserved          : int  = 0
 *  [16-19]  snapshotMaxOrders : int  — capacity this snapshot was taken with
 *  [20-23]  snapshotMaxChildren : int — capacity this snapshot was taken with
 *  [24 .. 24+orderCount*80]   parent order records (MESSAGE_SIZE each)
 *  [.. + dedupCount*16]       dedup entries (clOrdId:8 + orderId:8)
 *  [.. child registry: int childCount + childCount*128 bytes]
 */
public final class SnapshotManager {

    public static final int SNAPSHOT_VERSION   = 3;
    public static final int HEADER_SIZE        = 24; // extended from 16 to include capacity metadata
    public static final int DEDUP_ENTRY_SIZE   = 16; // clOrdId (8) + orderId (8)

    private final int maxOrders;
    private final int maxChildren;
    private final UnsafeBuffer snapshotBuffer;

    public SnapshotManager(final int maxOrders, final int maxChildren) {
        this.maxOrders  = maxOrders;
        this.maxChildren = maxChildren;
        final int bufSize =
                HEADER_SIZE
                + maxOrders  * OrderLayout.MESSAGE_SIZE
                + maxOrders  * DEDUP_ENTRY_SIZE
                + Integer.BYTES + maxChildren * OrderLayout.BLOCK_LENGTH;
        this.snapshotBuffer = new UnsafeBuffer(java.nio.ByteBuffer.allocateDirect(bufSize));
    }

    public SnapshotManager() {
        this(OrderBook.MAX_ORDERS, ChildOrderRegistry.DEFAULT_CAPACITY);
    }

    // ── Fragment handler for snapshot loading (allocated once at construction) ──
    private OrderBook           restoreTargetBook;
    private ValidationEngine    restoreTargetValidation;
    private ChildOrderRegistry  restoreTargetRegistry;
    private final FragmentHandler snapshotFragmentHandler = this::handleSnapshotFragment;

    // ── State for multi-fragment snapshot loading ──────────────────────────────
    private int  loadedOrders;
    private int  loadedDedupEntries;
    private int  expectedOrders;
    private int  expectedDedupEntries;
    private boolean headerLoaded;
    private boolean childRegistryRestored;

    // ── Serialization ─────────────────────────────────────────────────────────

    public void takeSnapshot(
            final OrderBook orderBook,
            final ValidationEngine validation,
            final ChildOrderRegistry childRegistry,
            final ExclusivePublication snapshotPublication,
            final IdleStrategy idleStrategy) {

        int position = 0;

        final int orderCount = orderBook.openOrderCount();
        final int dedupCount = validation.seenClOrdIds().size();

        snapshotBuffer.putInt(0,  SNAPSHOT_VERSION);
        snapshotBuffer.putInt(4,  orderCount);
        snapshotBuffer.putInt(8,  dedupCount);
        snapshotBuffer.putInt(12, 0); // reserved
        snapshotBuffer.putInt(16, maxOrders);
        snapshotBuffer.putInt(20, maxChildren);
        position = HEADER_SIZE;

        final int[] orderPosition = {position};
        orderBook.forEachActiveSlot(slot -> {
            snapshotBuffer.putBytes(
                    orderPosition[0],
                    orderBook.buffer(),
                    orderBook.offsetForSlot(slot),
                    OrderLayout.MESSAGE_SIZE);
            orderPosition[0] += OrderLayout.MESSAGE_SIZE;
        });
        position = orderPosition[0];

        final int[] dedupPosition = {position};
        validation.seenClOrdIds().forEach((clOrdId, orderId) -> {
            snapshotBuffer.putLong(dedupPosition[0],     clOrdId);
            snapshotBuffer.putLong(dedupPosition[0] + 8, orderId);
            dedupPosition[0] += DEDUP_ENTRY_SIZE;
        });
        position = dedupPosition[0];

        position += childRegistry.snapshot(snapshotBuffer, position);

        long result;
        do {
            result = snapshotPublication.offer(snapshotBuffer, 0, position);
            idleStrategy.idle();
        } while (result < 0);
    }

    // ── Deserialization ───────────────────────────────────────────────────────

    public void loadSnapshot(
            final Image snapshotImage,
            final OrderBook orderBook,
            final ValidationEngine validation,
            final ChildOrderRegistry childRegistry,
            final IdleStrategy idleStrategy) {

        orderBook.reset();
        validation.reset();

        this.restoreTargetBook       = orderBook;
        this.restoreTargetValidation = validation;
        this.restoreTargetRegistry   = childRegistry;
        this.headerLoaded            = false;
        this.loadedOrders            = 0;
        this.loadedDedupEntries      = 0;
        this.expectedOrders          = 0;
        this.expectedDedupEntries    = 0;
        this.childRegistryRestored   = false;

        while (!snapshotImage.isEndOfStream()) {
            final int fragments = snapshotImage.poll(snapshotFragmentHandler, 10);
            idleStrategy.idle(fragments);
        }
    }

    // ── Fragment handler ──────────────────────────────────────────────────────

    private void handleSnapshotFragment(
            final DirectBuffer buffer,
            final int offset,
            final int length,
            final Header header) {

        int pos = offset;

        if (!headerLoaded) {
            if (length < HEADER_SIZE) {
                throw new IllegalStateException("Snapshot header too short: " + length);
            }
            final int version = buffer.getInt(pos);
            if (version != SNAPSHOT_VERSION) {
                throw new IllegalStateException("Unsupported snapshot version: " + version
                        + " (expected " + SNAPSHOT_VERSION + "). Wipe archive with "
                        + "OMS_ARCHIVE_DELETE_ON_START=true and restart.");
            }
            expectedOrders       = buffer.getInt(pos + 4);
            expectedDedupEntries = buffer.getInt(pos + 8);
            // pos + 12 = reserved
            final int snapMaxOrders   = buffer.getInt(pos + 16);
            final int snapMaxChildren = buffer.getInt(pos + 20);
            if (snapMaxOrders > maxOrders) {
                throw new IllegalStateException(
                        "Snapshot maxOrders=" + snapMaxOrders
                        + " exceeds current OMS_MAX_ORDERS=" + maxOrders
                        + ". Increase OMS_MAX_ORDERS or wipe archive.");
            }
            if (snapMaxChildren > maxChildren) {
                throw new IllegalStateException(
                        "Snapshot maxChildren=" + snapMaxChildren
                        + " exceeds current OMS_MAX_CHILDREN=" + maxChildren
                        + ". Increase OMS_MAX_CHILDREN or wipe archive.");
            }
            pos += HEADER_SIZE;
            headerLoaded = true;
        }

        while (loadedOrders < expectedOrders
               && pos + OrderLayout.MESSAGE_SIZE <= offset + length) {
            restoreTargetBook.restoreOrder(buffer, pos);
            pos += OrderLayout.MESSAGE_SIZE;
            loadedOrders++;
        }

        while (loadedDedupEntries < expectedDedupEntries
               && pos + DEDUP_ENTRY_SIZE <= offset + length) {
            final long clOrdId = buffer.getLong(pos);
            final long orderId = buffer.getLong(pos + 8);
            restoreTargetValidation.restoreEntry(clOrdId, orderId);
            pos += DEDUP_ENTRY_SIZE;
            loadedDedupEntries++;
        }

        if (!childRegistryRestored
                && loadedDedupEntries == expectedDedupEntries
                && restoreTargetRegistry != null
                && pos + Integer.BYTES <= offset + length) {
            restoreTargetRegistry.restore(buffer, pos, restoreTargetBook);
            childRegistryRestored = true;
        }
    }
}
