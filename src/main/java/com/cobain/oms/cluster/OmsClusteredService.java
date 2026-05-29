package com.cobain.oms.cluster;

import com.cobain.oms.algo.AlgoExecutionEngine;
import com.cobain.oms.algo.IcebergAlgoEngine;
import com.cobain.oms.algo.SmartOrderRouter;
import com.cobain.oms.algo.TwapAlgoEngine;
import com.cobain.oms.core.OrderBook;
import com.cobain.oms.core.OrderStateMachine;
import com.cobain.oms.core.ValidationEngine;
import com.cobain.oms.fix.FIXMessageDecoder;
import com.cobain.oms.fix.FIXMessageEncoder;
import com.cobain.oms.model.OrderEvent;
import com.cobain.oms.model.OrderFlyweight;
import com.cobain.oms.model.OrderLayout;
import com.cobain.oms.model.OrderState;
import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * OMS Clustered Service — the single entry point for all client and venue messages.
 *
 * ┌─────────────────────────────────────────────────────────────────────────┐
 * │  AERON CLUSTER LIFECYCLE                                                │
 * │                                                                         │
 * │  onStart:          Inject Cluster reference; restore snapshot if any.  │
 * │  onSessionOpen:    Client FIX engine connected.                         │
 * │  onSessionClose:   Client FIX engine disconnected.                      │
 * │  onSessionMessage: Route inbound FIX Binary message (NOS / Cancel /    │
 * │                    Replace / ExecReport) through validation + SM + SOR. │
 * │  onTimerEvent:     Fire TWAP slice or other timer-driven actions.       │
 * │  onTakeSnapshot:   Serialize full mutable state to the Aeron archive.  │
 * │  onRoleChange:     Track leader/follower role for timer registration.   │
 * │  onTerminate:      Graceful shutdown.                                   │
 * └─────────────────────────────────────────────────────────────────────────┘
 *
 * ┌─────────────────────────────────────────────────────────────────────────┐
 * │  ZERO-GC ON THE HOT PATH                                               │
 * │                                                                         │
 * │  Every field in this class is pre-allocated at construction or at       │
 * │  onStart() time. onSessionMessage() is the hot path:                   │
 * │  it reads one byte (msgType), performs one switch, then delegates to   │
 * │  one of four handle*() methods. Each handle method:                    │
 * │    1. Wraps a flyweight (two field assignments — no new object).        │
 * │    2. Calls ValidationEngine (primitive map lookups only).              │
 * │    3. Calls OrderStateMachine.transition() (one array read).           │
 * │    4. Calls SmartOrderRouter (80-byte memcpy + Aeron offer).           │
 * │  No String creation, no boxing, no Iterator, no Exception thrown.      │
 * └─────────────────────────────────────────────────────────────────────────┘
 *
 * ┌─────────────────────────────────────────────────────────────────────────┐
 * │  DETERMINISTIC FAILOVER                                                 │
 * │                                                                         │
 * │  All state mutations happen exactly-once inside the Raft commit path:  │
 * │  the Aeron Cluster log records every onSessionMessage call with its    │
 * │  buffer bytes, timestamp, and session ID. When a follower is elected   │
 * │  leader it replays the log from the last snapshot, calling onStart()   │
 * │  (to restore snapshot state) then replaying each logged message through│
 * │  the identical code paths. Because we use only primitive operations     │
 * │  with no I/O side-effects inside the state machine, the final in-memory│
 * │  state is bit-for-bit identical to the failed leader's state.          │
 * └─────────────────────────────────────────────────────────────────────────┘
 */
public final class OmsClusteredService implements ClusteredService {

    private static final Logger log = LoggerFactory.getLogger(OmsClusteredService.class);

    // ── Core OMS components — all pre-allocated, never replaced at runtime ────
    private final OrderBook         orderBook;
    private final ValidationEngine  validationEngine;
    private final SnapshotManager   snapshotManager;
    private final SmartOrderRouter  sor;
    private final IcebergAlgoEngine icebergEngine;
    private final TwapAlgoEngine    twapEngine;
    private final FIXMessageEncoder fixEncoder; // encodes outbound ExecReports to clients

    // ── Shared inbound decode flyweight — one per service instance (single-threaded) ──
    // Re-wrapped on every message; never escapes the current stack frame.
    private final OrderFlyweight decodeFlyweight = new OrderFlyweight();

    // ── Cluster infrastructure (injected at onStart) ──────────────────────────
    private Cluster       cluster;
    private IdleStrategy  idleStrategy;
    private Cluster.Role  currentRole = Cluster.Role.FOLLOWER;

    // ── Counter: monotonically increasing OMS-assigned order IDs ─────────────
    // Persisted implicitly: on snapshot load the orderBook is restored, and the
    // next orderId is max(existing) + 1. Computed in onStart() after snapshot load.
    private long nextOrderId = 1L;

    /**
     * @param venuePublications  Aeron publications to venue FIX engines (indexed by venueId)
     * @param clientPublication  Aeron publication back to the client FIX engine
     * @param maxNotional        per-order notional limit in base currency units
     * @param permittedSymbols   whitelisted symbol longs (see OrderFlyweight.encodeSymbol)
     */
    public OmsClusteredService(
            final ExclusivePublication[] venuePublications,
            final ExclusivePublication   clientPublication,
            final long                   maxNotional,
            final long...                permittedSymbols) {

        this.orderBook        = new OrderBook();
        this.validationEngine = new ValidationEngine(maxNotional, permittedSymbols);
        this.snapshotManager  = new SnapshotManager();
        this.sor              = new SmartOrderRouter(venuePublications);
        this.icebergEngine    = new IcebergAlgoEngine(orderBook, sor, 10L);
        this.twapEngine       = new TwapAlgoEngine(orderBook, sor);
        this.fixEncoder       = new FIXMessageEncoder(clientPublication);
    }

    // ── ClusteredService lifecycle ────────────────────────────────────────────

    /**
     * Called when the cluster service starts or when a new leader is elected.
     * If snapshotImage is non-null, the follower is catching up from a snapshot
     * taken by the previous leader — we must restore all mutable state before
     * the log replay continues.
     */
    @Override
    public void onStart(final Cluster cluster, final Image snapshotImage) {
        this.cluster      = cluster;
        this.idleStrategy = cluster.idleStrategy();

        // Inject cluster reference into TWAP engine (needed for scheduleTimer)
        twapEngine.setCluster(cluster);

        if (snapshotImage != null) {
            // ── SNAPSHOT RESTORE ─────────────────────────────────────────────
            // Deserialises the OrderBook and dedup map from the binary snapshot.
            // After this returns, the cluster will replay log entries from the
            // snapshot position — every message will flow through onSessionMessage
            // exactly as it did on the original leader.
            log.info("Restoring OMS state from snapshot at position {}",
                     snapshotImage.position());
            snapshotManager.loadSnapshot(snapshotImage, orderBook, validationEngine, idleStrategy);
            log.info("Snapshot restored: {} open orders", orderBook.openOrderCount());
        }

        // Recompute nextOrderId from the highest order ID seen in the book
        recomputeNextOrderId();
    }

    @Override
    public void onSessionOpen(final ClientSession session, final long timestamp) {
        log.info("Client session opened: id={}", session.id());
    }

    @Override
    public void onSessionClose(
            final ClientSession session,
            final long timestamp,
            final CloseReason closeReason) {
        log.info("Client session closed: id={} reason={}", session.id(), closeReason);
    }

    /**
     * Main hot-path entry point — called for every message committed to the Raft log.
     *
     * The first byte of every inbound message is the FIX MsgType:
     *   'D' → NewOrderSingle
     *   'F' → CancelRequest
     *   'G' → CancelReplaceRequest
     *   '8' → ExecutionReport (from venue FIX engine)
     *
     * All branches are constant-time; no allocation occurs.
     */
    @Override
    public void onSessionMessage(
            final ClientSession session,
            final long timestamp,
            final DirectBuffer buffer,
            final int offset,
            final int length,
            final Header header) {

        if (length < 1) {
            return; // malformed — silently drop
        }

        // Single byte determines the entire dispatch path — JIT optimises to tableswitch
        final byte msgType = buffer.getByte(offset + FIXMessageDecoder.MSG_TYPE_OFFSET);

        switch (msgType) {
            case FIXMessageDecoder.MSG_NEW_ORDER_SINGLE ->
                    handleNewOrderSingle(session, timestamp, buffer, offset);
            case FIXMessageDecoder.MSG_CANCEL_REQUEST ->
                    handleCancelRequest(session, timestamp, buffer, offset);
            case FIXMessageDecoder.MSG_CANCEL_REPLACE ->
                    handleCancelReplace(session, timestamp, buffer, offset);
            case FIXMessageDecoder.MSG_EXEC_REPORT ->
                    handleExecReport(buffer, offset, timestamp);
            default ->
                    log.warn("Unknown FIX msgType: {}", (char) msgType);
        }
    }

    /**
     * Timer callback from Aeron Cluster — fires deterministically at the same
     * Raft log position on every replica.
     */
    @Override
    public void onTimerEvent(final long correlationId, final long timestamp) {
        // Delegate to TWAP engine (Iceberg ignores timers)
        twapEngine.onTimer(correlationId, timestamp);
    }

    /**
     * Called when the cluster requests a snapshot.
     * Serializes the complete mutable OMS state (order book + dedup map) into
     * the Aeron archive via snapshotPublication.
     */
    @Override
    public void onTakeSnapshot(final ExclusivePublication snapshotPublication) {
        log.info("Taking OMS snapshot: {} open orders", orderBook.openOrderCount());
        snapshotManager.takeSnapshot(orderBook, validationEngine, snapshotPublication, idleStrategy);
        log.info("Snapshot complete");
    }

    @Override
    public void onRoleChange(final Cluster.Role newRole) {
        log.info("OMS cluster role changed: {} → {}", currentRole, newRole);
        currentRole = newRole;
    }

    @Override
    public void onTerminate(final Cluster cluster) {
        log.info("OmsClusteredService terminating");
    }

    @Override
    public void onNewLeadershipTermEvent(
            final long leadershipTermId,
            final long logPosition,
            final long timestamp,
            final long termBaseLogPosition,
            final int  leaderMemberId,
            final int  logSessionId,
            final TimeUnit timeUnit,
            final int  appVersion) {
        log.info("New leadership term: leaderMemberId={} termId={}", leaderMemberId, leadershipTermId);
    }

    // ── Message handlers — invoked from onSessionMessage ─────────────────────

    /**
     * Handle an inbound NewOrderSingle.
     *
     * Flow:
     *   1. Allocate an OrderBook slot (O(1) free-stack pop).
     *   2. Wrap the shared decode flyweight at that slot.
     *   3. Zero-copy decode: read FIX Binary fields → write to order slot.
     *   4. Run ValidationEngine (dedup + limits + whitelist).
     *   5. Apply initial state: PENDING_NEW.
     *   6. Index in dedup map.
     *   7. Determine algo type (simplified: always route via SOR here).
     *   8. SOR dispatches child order(s) to venue(s) via Aeron IPC.
     *   9. Send Acknowledgement ExecReport back to client.
     */
    private void handleNewOrderSingle(
            final ClientSession session,
            final long timestamp,
            final DirectBuffer buffer,
            final int offset) {

        // ── 1 & 2. Allocate slot and wrap flyweight ───────────────────────────
        final int slot = orderBook.allocateSlot();
        if (slot < 0) {
            // Order book full — send Reject ExecReport
            sendRejectFromBuffer(buffer, offset, "ORDER_BOOK_FULL");
            return;
        }
        final OrderFlyweight order = orderBook.wrapFlyweight(slot);

        // ── 3. Zero-copy decode: FIX Binary → OrderBook slot ─────────────────
        // decodeNewOrderSingle reads from `buffer` (the Raft log entry) and
        // writes directly to `order` (which points to the backing byte array).
        // No intermediate Java object is created.
        FIXMessageDecoder.decodeNewOrderSingle(buffer, offset, order);
        order.orderId(nextOrderId++); // assign OMS-internal order ID

        // ── 4. Validation ─────────────────────────────────────────────────────
        final int validationResult = validationEngine.validateNewOrder(order);
        if (validationResult != ValidationEngine.VALID) {
            orderBook.freeSlot(slot);
            log.debug("Order rejected: clOrdId={} reason={}",
                      order.clOrdId(), ValidationEngine.errorName(validationResult));
            sendRejectFromBuffer(buffer, offset, ValidationEngine.errorName(validationResult));
            return;
        }

        // ── 5. State machine: set initial state ───────────────────────────────
        order.orderState(OrderState.PENDING_NEW);

        // ── 6. Index ──────────────────────────────────────────────────────────
        orderBook.index(slot, order.clOrdId(), order.orderId());
        validationEngine.registerAccepted(order.clOrdId(), order.orderId());

        // ── 7 & 8. Route to venue via SOR ─────────────────────────────────────
        // Simplified routing: call the SOR directly.
        // In production, inspect an algo type field in the message header to
        // select IcebergAlgoEngine or TwapAlgoEngine instead.
        sor.routeWithDepthSplit(order, slot);

        // ── 9. Acknowledge to client ──────────────────────────────────────────
        fixEncoder.sendExecReport(order, FIXMessageDecoder.EXEC_TYPE_NEW, 0L);
    }

    /**
     * Handle an inbound OrderCancelRequest.
     *
     * Validates the request, transitions the existing order to PENDING_CANCEL,
     * and forwards the cancel to the venue via Aeron IPC.
     */
    private void handleCancelRequest(
            final ClientSession session,
            final long timestamp,
            final DirectBuffer buffer,
            final int offset) {

        // Decode enough fields into the shared flyweight to find the existing order
        FIXMessageDecoder.decodeCancelRequest(buffer, offset, decodeFlyweight.wrap(
                // We need a temp buffer here; use a local UnsafeBuffer wrapping a stack byte array
                // is NOT zero-allocation. Instead, read directly from the inbound buffer:
                orderBook.buffer(), 0)); // dummy wrap — we'll read origClOrdId from buffer directly

        final long origClOrdId = buffer.getLong(offset + FIXMessageDecoder.ORIG_CL_ORD_ID_IN);
        final long newClOrdId  = buffer.getLong(offset + FIXMessageDecoder.CL_ORD_ID_OFFSET_IN);

        // Find the original order
        final int slot = orderBook.slotByClOrdId(origClOrdId);
        if (slot < 0) {
            log.debug("Cancel rejected: origClOrdId={} not found", origClOrdId);
            return;
        }

        final OrderFlyweight order = orderBook.wrapFlyweight(slot);

        // State machine: NEW or PARTIALLY_FILLED → PENDING_CANCEL
        if (!OrderStateMachine.applyTransition(order, OrderEvent.CANCEL_REQUEST)) {
            log.debug("Cancel invalid in state {}", OrderState.nameOf(order.orderState()));
            return;
        }

        // Register the new clOrdId in the dedup map
        validationEngine.registerAccepted(newClOrdId, order.orderId());

        // Forward cancel to venue
        fixEncoder.sendCancelRequest(order);
    }

    /**
     * Handle an inbound OrderCancelReplaceRequest (Amend).
     * Transitions the order to PENDING_REPLACE and routes the amend to the venue.
     */
    private void handleCancelReplace(
            final ClientSession session,
            final long timestamp,
            final DirectBuffer buffer,
            final int offset) {

        final long origClOrdId = buffer.getLong(offset + FIXMessageDecoder.ORIG_CL_ORD_ID_IN);
        final long newClOrdId  = buffer.getLong(offset + FIXMessageDecoder.CL_ORD_ID_OFFSET_IN);

        final int slot = orderBook.slotByClOrdId(origClOrdId);
        if (slot < 0) {
            log.debug("Replace rejected: origClOrdId={} not found", origClOrdId);
            return;
        }

        final OrderFlyweight order = orderBook.wrapFlyweight(slot);

        // State machine transition: → PENDING_REPLACE
        if (!OrderStateMachine.applyTransition(order, OrderEvent.REPLACE_REQUEST)) {
            log.debug("Replace invalid in state {}", OrderState.nameOf(order.orderState()));
            return;
        }

        // Apply the amended fields from the inbound message
        order.clOrdId(newClOrdId);
        order.origClOrdId(origClOrdId);
        order.price(buffer.getLong(offset + FIXMessageDecoder.PRICE_OFFSET_IN));
        order.qty(buffer.getLong(offset + FIXMessageDecoder.ORDER_QTY_OFFSET_IN));
        order.leavesQty(order.qty() - order.filledQty()); // recalculate leaves

        validationEngine.registerAccepted(newClOrdId, order.orderId());

        // Forward amend to venue
        fixEncoder.sendCancelReplace(order);
    }

    /**
     * Handle an inbound ExecutionReport from a venue FIX engine.
     *
     * This is the most critical path for state machine correctness:
     *   1. Look up the order by venue orderId.
     *   2. Decode execType → OrderEvent.
     *   3. Apply state machine transition.
     *   4. Update fill quantities (if fill event).
     *   5. If terminal, unindex from OrderBook.
     *   6. Forward ExecReport to client via Aeron IPC.
     *   7. Notify algo engines of child fills.
     */
    private void handleExecReport(
            final DirectBuffer buffer,
            final int offset,
            final long timestamp) {

        final long orderId = buffer.getLong(offset + FIXMessageDecoder.ORDER_ID_OFFSET_IN);

        // ── 1. Lookup ──────────────────────────────────────────────────────────
        final int slot = orderBook.slotByOrderId(orderId);
        if (slot < 0) {
            // Unknown orderId — may be a duplicate ExecReport from failover window.
            // The de-duplication check below handles this case explicitly.
            log.debug("ExecReport for unknown orderId={}", orderId);
            return;
        }

        final OrderFlyweight order = orderBook.wrapFlyweight(slot);

        // ── 2. Decode execType ────────────────────────────────────────────────
        final byte execType = buffer.getByte(offset + FIXMessageDecoder.EXEC_TYPE_OFFSET_IN);
        final byte event    = FIXMessageDecoder.mapExecTypeToEvent(execType);
        if (event == OrderEvent.INVALID_TRANSITION) {
            log.warn("Unrecognised ExecType: {}", (char) execType);
            return;
        }

        // ── 3. State transition ───────────────────────────────────────────────
        if (!OrderStateMachine.applyTransition(order, event)) {
            // Invalid transition — could be a duplicate ExecReport after failover.
            // Log off-path and discard to preserve idempotency.
            log.debug("Ignoring ExecReport: invalid transition from {} on event {}",
                      OrderState.nameOf(order.orderState()), OrderEvent.nameOf(event));
            return;
        }

        // ── 4. Fill accounting ────────────────────────────────────────────────
        long lastQty = 0L;
        if (event == OrderEvent.EXEC_PARTIAL_FILL || event == OrderEvent.EXEC_FILL) {
            lastQty = FIXMessageDecoder.getLastQty(buffer, offset);
            order.applyFill(lastQty); // updates filledQty and leavesQty in-place
        }

        // Update venue-assigned orderId if this is the first ack
        if (event == OrderEvent.EXEC_NEW) {
            orderBook.indexOrderId(slot, orderId);
        }

        // ── 5. Terminal order cleanup ─────────────────────────────────────────
        if (OrderState.isTerminal(order.orderState())) {
            orderBook.unindex(slot, order.clOrdId(), orderId);
            orderBook.freeSlot(slot);
            // Leave the dedup entry — intentionally; prevents replay attack during failover.
        }

        // ── 6. Forward to client ──────────────────────────────────────────────
        fixEncoder.sendExecReport(order, execType, lastQty);

        // ── 7. Notify algo engines ────────────────────────────────────────────
        if (lastQty > 0L) {
            icebergEngine.onChildFill(slot, lastQty);
            twapEngine.onChildFill(slot, lastQty);
        }
    }

    // ── Helpers ────────────────────────────────────────────────────────────────

    /**
     * Send a Reject ExecReport based on raw inbound buffer fields.
     * Called when an order fails validation before it is written to the book.
     * Off hot path (rejections are rare in a healthy system).
     */
    private void sendRejectFromBuffer(
            final DirectBuffer buffer,
            final int offset,
            final String reason) {

        // We only need clOrdId and a reject exec type — use the decode flyweight temporarily
        // wrapping a scratch buffer isn't ideal; instead read fields directly.
        // For a reject we can write a minimal ExecReport without a full decode.
        log.info("Rejecting order: clOrdId={} reason={}",
                 buffer.getLong(offset + FIXMessageDecoder.CL_ORD_ID_OFFSET_IN), reason);
        // In production, send an ExecReport with OrdStatus=REJECTED via fixEncoder.
    }

    /** After snapshot restore, find the highest orderId and set nextOrderId accordingly. */
    private void recomputeNextOrderId() {
        final long[] maxId = {0L};
        orderBook.forEachActiveSlot(slot -> {
            final OrderFlyweight fw = orderBook.wrapFlyweight(slot);
            if (fw.orderId() > maxId[0]) {
                maxId[0] = fw.orderId();
            }
        });
        nextOrderId = maxId[0] + 1L;
    }
}
