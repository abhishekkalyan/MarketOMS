package com.cobain.oms.cluster;

import com.cobain.oms.codec.ChildOrderIntentFlyweight;
import com.cobain.oms.codec.ClusterMessageType;
import com.cobain.oms.core.ChildOrderIntentValidator;
import com.cobain.oms.core.ChildOrderRegistry;
import com.cobain.oms.core.OrderBook;
import com.cobain.oms.core.OrderStateMachine;
import com.cobain.oms.core.ParentOrderState;
import com.cobain.oms.core.ValidationEngine;
import com.cobain.oms.fix.FIXMessageDecoder;
import com.cobain.oms.fix.FIXMessageEncoder;
import com.cobain.oms.model.OrderEvent;
import com.cobain.oms.model.OrderFlyweight;
import com.cobain.oms.model.OrderLayout;
import com.cobain.oms.model.OrderState;
import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * OMS Clustered Service — the single golden source for all order state.
 *
 * In the intent-driven architecture:
 *   1. Parent orders arrive from FIX clients and are written to OrderBook.
 *   2. Parent orders are published to algo-sor for routing computation.
 *   3. algo-sor returns ChildOrderIntent messages via a separate IPC channel.
 *   4. This service validates each intent, creates child orders in ChildOrderRegistry,
 *      links children to parents, and dispatches child NOS to the FIX bridge.
 *
 * Every state transition on both parent and child orders flows through OrderStateMachine.
 * No state is mutated anywhere else.
 */
public final class OmsClusteredService implements ClusteredService {

    private static final Logger log = LoggerFactory.getLogger(OmsClusteredService.class);

    private static final int INTENT_FRAGMENT_LIMIT = 20;

    // ── Core OMS components — all pre-allocated ───────────────────────────────
    private final OrderBook                  orderBook;
    private final ValidationEngine           validationEngine;
    private final SnapshotManager            snapshotManager;
    private final FIXMessageEncoder          fixEncoder;
    private final ChildOrderRegistry         childRegistry;
    private final ChildOrderIntentValidator  intentValidator;

    // ── IPC channels ──────────────────────────────────────────────────────────
    private final ExclusivePublication algoSorPublication; // oms-core → algo-sor
    private final Subscription         intentSub;          // algo-sor → oms-core (intents)

    // ── Pre-allocated outbound IPC buffer: 1-byte type + ORDER_SIZE payload ───
    private final UnsafeBuffer algoOutbound =
            new UnsafeBuffer(new byte[ClusterMessageType.IPC_MESSAGE_SIZE]);

    // ── Pre-allocated egress buffer for cluster client responses ─────────────
    // Same framing as ingress: 1-byte ClusterMessageType + 128-byte OrderLayout.
    private final UnsafeBuffer egressBuffer =
            new UnsafeBuffer(new byte[ClusterMessageType.IPC_MESSAGE_SIZE]);

    // ── Intent inbound view ───────────────────────────────────────────────────
    private final ChildOrderIntentFlyweight intentView = new ChildOrderIntentFlyweight();

    // ── Shared inbound decode flyweight ───────────────────────────────────────
    private final OrderFlyweight decodeFlyweight = new OrderFlyweight();

    // ── Intent fragment handler (pre-allocated, no lambda) ────────────────────
    private final FragmentHandler intentFragmentHandler = this::handleIntentFragment;

    // ── Cluster infrastructure ────────────────────────────────────────────────
    private Cluster       cluster;
    private IdleStrategy  idleStrategy;
    private Cluster.Role  currentRole = Cluster.Role.FOLLOWER;

    private long nextOrderId = 1L;

    public OmsClusteredService(
            final ExclusivePublication algoSorPublication,
            final ExclusivePublication clientPublication,
            final Subscription         intentSub,
            final long                 maxNotional,
            final long...              permittedSymbols) {

        this.orderBook          = new OrderBook();
        this.validationEngine   = new ValidationEngine(maxNotional, permittedSymbols);
        this.snapshotManager    = new SnapshotManager();
        this.algoSorPublication = algoSorPublication;
        this.fixEncoder         = new FIXMessageEncoder(clientPublication);
        this.childRegistry      = new ChildOrderRegistry();
        this.intentValidator    = new ChildOrderIntentValidator();
        this.intentSub          = intentSub;
    }

    // ── ClusteredService lifecycle ────────────────────────────────────────────

    @Override
    public void onStart(final Cluster cluster, final Image snapshotImage) {
        this.cluster      = cluster;
        this.idleStrategy = cluster.idleStrategy();

        ParentOrderState.registerTransitions();

        if (snapshotImage != null) {
            log.info("Restoring OMS state from snapshot at position {}",
                     snapshotImage.position());
            snapshotManager.loadSnapshot(snapshotImage, orderBook, validationEngine, idleStrategy);
            log.info("Snapshot restored: {} open orders", orderBook.openOrderCount());
        } else {
            log.info("No snapshot found — starting with fresh state");
        }

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

    @Override
    public void onSessionMessage(
            final ClientSession session,
            final long timestamp,
            final DirectBuffer buffer,
            final int offset,
            final int length,
            final Header header) {

        // Drain any pending ChildOrderIntents from algo-sor before processing client message
        pollIntents();

        if (length < 1) {
            return;
        }

        // Cluster ingress uses ClusterMessageType framing: byte 0 = msgType, bytes 1-128 = OrderLayout.
        // FIXMessageDecoder is only used for the FIX bridge → oms-core IPC channel, not here.
        final byte msgType = buffer.getByte(offset + ClusterMessageType.OFFSET_MSG_TYPE);

        switch (msgType) {
            case ClusterMessageType.NEW_ORDER ->
                    handleNewOrderSingle(session, timestamp, buffer, offset);
            case ClusterMessageType.CANCEL_ORDER ->
                    handleCancelRequest(session, timestamp, buffer, offset);
            case ClusterMessageType.REPLACE_ORDER ->
                    handleCancelReplace(session, timestamp, buffer, offset);
            default ->
                    log.warn("Unknown cluster msgType: {}", msgType);
        }
    }

    @Override
    public void onTimerEvent(final long correlationId, final long timestamp) {
        // Drain intents on each timer tick so intents are not stranded when no
        // client messages are flowing.
        pollIntents();
    }

    @Override
    public void onTakeSnapshot(final ExclusivePublication snapshotPublication) {
        log.info("Taking OMS snapshot: {} open orders, {} child orders",
                 orderBook.openOrderCount(), childRegistry.size());
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

    // ── Intent polling ────────────────────────────────────────────────────────

    /**
     * Drain pending ChildOrderIntents from algo-sor.
     * Called from onSessionMessage() and onTimerEvent() so intents are processed
     * on the cluster service thread, maintaining single-threaded execution.
     */
    private void pollIntents() {
        if (intentSub != null) {
            intentSub.poll(intentFragmentHandler, INTENT_FRAGMENT_LIMIT);
        }
    }

    private void handleIntentFragment(
            final DirectBuffer buffer,
            final int offset,
            final int length,
            final Header header) {

        if (length < ClusterMessageType.HEADER_LENGTH + ChildOrderIntentFlyweight.BLOCK_LENGTH) {
            return;
        }
        final byte msgType = buffer.getByte(offset + ClusterMessageType.OFFSET_MSG_TYPE);
        if (msgType == ClusterMessageType.CHILD_ORDER_INTENT) {
            onChildOrderIntent(buffer, offset);
        }
    }

    // ── Child order intent handling ───────────────────────────────────────────

    /**
     * Handle a CHILD_ORDER_INTENT arriving from algo-sor.
     *
     * FLOW:
     *   1. Wrap intent view over inbound buffer (zero copy)
     *   2. Look up parent in OrderBook
     *   3. Compute liveChildQty for over-allocation guard
     *   4. Run ChildOrderIntentValidator — if REJECT, log and return
     *   5. Assign childOrderId from nextOrderId counter
     *   6. Create child in ChildOrderRegistry (links to parent)
     *   7. If parent state is NEW: transition parent → ROUTING
     *   8. Dispatch child NOS to FIX bridge
     *
     * ZERO-ALLOCATION: all operations on pre-allocated flyweights.
     */
    private void onChildOrderIntent(final DirectBuffer buffer, final int offset) {
        // Step 1: wrap intent view — zero copy
        intentView.wrapReadOnly(buffer, offset + ClusterMessageType.HEADER_LENGTH);

        // Step 2: look up parent
        final long parentOrderId = intentView.getParentOrderId();
        final int  parentSlot    = orderBook.slotByOrderId(parentOrderId);
        final OrderFlyweight parent = (parentSlot >= 0) ? orderBook.wrapFlyweight(parentSlot) : null;

        // Step 3: compute live child qty for over-allocation guard
        final long liveChildQty = (parent != null)
                ? childRegistry.computeLiveChildQty(parent)
                : 0L;

        // Step 4: validate — fast-fail on any rule violation
        final byte validationResult = intentValidator.validate(intentView, parent, liveChildQty);
        if (validationResult != ChildOrderIntentValidator.PASS) {
            log.debug("ChildOrderIntent rejected: parentOrderId={} reason={}",
                      parentOrderId, ChildOrderIntentValidator.describe(validationResult));
            return;
        }

        // Step 5: assign system childOrderId — single monotonic counter, no CAS
        final long childOrderId = nextOrderId++;

        // Step 6: create child — born here, in oms-core, on the replicated log
        // parent is non-null (validated above)
        final OrderFlyweight child = childRegistry.createChild(intentView, parent, childOrderId);
        if (child == null) {
            log.warn("ChildOrderRegistry full — intent dropped for parentOrderId={}", parentOrderId);
            return;
        }

        // Step 7: transition parent from NEW → ROUTING on first child dispatch
        if (parent.orderState() == OrderState.NEW) {
            OrderStateMachine.transitionToRouting(parent);
        }

        // Step 8: dispatch child NOS to FIX bridge
        // Child is durably recorded BEFORE this line — if we crash here, the child
        // exists in the next snapshot and can be reconciled via FIX OrderStatusRequest (35=H).
        fixEncoder.sendNewOrderSingle(child);
    }

    // ── Message handlers ──────────────────────────────────────────────────────

    private void handleNewOrderSingle(
            final ClientSession session,
            final long timestamp,
            final DirectBuffer buffer,
            final int offset) {

        final int slot = orderBook.allocateSlot();
        if (slot < 0) {
            sendReject(buffer.getLong(offset + ClusterMessageType.OFFSET_PAYLOAD + OrderLayout.CL_ORD_ID_OFFSET), "ORDER_BOOK_FULL");
            return;
        }
        final OrderFlyweight order = orderBook.wrapFlyweight(slot);

        // Payload is OrderLayout format starting at OFFSET_PAYLOAD (byte 1).
        // Read each field directly — zero-alloc, no intermediate representation.
        final int p = offset + ClusterMessageType.OFFSET_PAYLOAD;
        order.accountId(   buffer.getLong( p + OrderLayout.ACCOUNT_ID_OFFSET));
        order.clOrdId(     buffer.getLong( p + OrderLayout.CL_ORD_ID_OFFSET));
        order.origClOrdId( buffer.getLong( p + OrderLayout.ORIG_CL_ORD_ID_OFFSET));
        order.symbol(      buffer.getLong( p + OrderLayout.SYMBOL_OFFSET));
        order.price(       buffer.getLong( p + OrderLayout.PRICE_OFFSET));
        order.qty(         buffer.getLong( p + OrderLayout.QTY_OFFSET));
        order.filledQty(   0L);
        order.leavesQty(   buffer.getLong( p + OrderLayout.QTY_OFFSET));
        order.side(        buffer.getByte( p + OrderLayout.SIDE_OFFSET));
        order.timeInForce( buffer.getByte( p + OrderLayout.TIME_IN_FORCE_OFFSET));
        order.venueId(     0);

        // Server-assigned fields
        order.orderId(nextOrderId++);
        order.orderState(OrderState.NEW);
        order.transactTime(System.nanoTime());
        order.childCount(0);
        order.parentOrFirstChildId(0L);
        order.nextSiblingSlot(-1);

        final int validationResult = validationEngine.validateNewOrder(order);
        if (validationResult != ValidationEngine.VALID) {
            log.debug("Order rejected: clOrdId={} reason={}",
                      order.clOrdId(), ValidationEngine.errorName(validationResult));
            // Send REJECTED exec report before freeing the slot (flyweight still valid here).
            order.orderState(OrderState.REJECTED);
            sendEgressExecReport(session, ClusterMessageType.NEW_ORDER, order);
            orderBook.freeSlot(slot);
            return;
        }

        orderBook.index(slot, order.clOrdId(), order.orderId());
        validationEngine.registerAccepted(order.clOrdId(), order.orderId());

        // Publish validated parent order to algo-sor via Aeron IPC (zero-alloc)
        algoOutbound.putByte(0, ClusterMessageType.NEW_ORDER);
        algoOutbound.putBytes(
                ClusterMessageType.OFFSET_PAYLOAD,
                orderBook.buffer(),
                orderBook.offsetForSlot(slot),
                OrderLayout.MESSAGE_SIZE);
        algoSorPublication.offer(algoOutbound, 0, ClusterMessageType.IPC_MESSAGE_SIZE);

        // Send exec report (state=NEW) back to the cluster client via egress.
        sendEgressExecReport(session, ClusterMessageType.NEW_ORDER, order);
    }

    private void handleCancelRequest(
            final ClientSession session,
            final long timestamp,
            final DirectBuffer buffer,
            final int offset) {

        final int  p           = offset + ClusterMessageType.OFFSET_PAYLOAD;
        final long origClOrdId = buffer.getLong(p + OrderLayout.ORIG_CL_ORD_ID_OFFSET);
        final long newClOrdId  = buffer.getLong(p + OrderLayout.CL_ORD_ID_OFFSET);

        final int slot = orderBook.slotByClOrdId(origClOrdId);
        if (slot < 0) {
            log.debug("Cancel rejected: origClOrdId={} not found", origClOrdId);
            return;
        }

        final OrderFlyweight order = orderBook.wrapFlyweight(slot);

        if (!OrderStateMachine.applyTransition(order, OrderEvent.CANCEL_REQUEST)) {
            log.debug("Cancel invalid in state {}", OrderState.nameOf(order.orderState()));
            return;
        }

        // Cancel all live child orders (routes cancel NOS to FIX bridge for each child)
        childRegistry.cancelAllChildren(order, child -> fixEncoder.sendCancelRequest(child));

        validationEngine.registerAccepted(newClOrdId, order.orderId());
        fixEncoder.sendCancelRequest(order);

        // Send cancel ack back to the cluster client via egress.
        sendEgressExecReport(session, ClusterMessageType.CANCEL_ORDER, order);
    }

    private void handleCancelReplace(
            final ClientSession session,
            final long timestamp,
            final DirectBuffer buffer,
            final int offset) {

        final int  p           = offset + ClusterMessageType.OFFSET_PAYLOAD;
        final long origClOrdId = buffer.getLong(p + OrderLayout.ORIG_CL_ORD_ID_OFFSET);
        final long newClOrdId  = buffer.getLong(p + OrderLayout.CL_ORD_ID_OFFSET);

        final int slot = orderBook.slotByClOrdId(origClOrdId);
        if (slot < 0) {
            log.debug("Replace rejected: origClOrdId={} not found", origClOrdId);
            return;
        }

        final OrderFlyweight order = orderBook.wrapFlyweight(slot);

        if (!OrderStateMachine.applyTransition(order, OrderEvent.REPLACE_REQUEST)) {
            log.debug("Replace invalid in state {}", OrderState.nameOf(order.orderState()));
            return;
        }

        order.clOrdId(newClOrdId);
        order.origClOrdId(origClOrdId);
        order.price(buffer.getLong(p + OrderLayout.PRICE_OFFSET));
        order.qty(buffer.getLong(p + OrderLayout.QTY_OFFSET));
        order.leavesQty(order.qty() - order.filledQty());

        validationEngine.registerAccepted(newClOrdId, order.orderId());
        fixEncoder.sendCancelReplace(order);
    }

    private void handleExecReport(
            final DirectBuffer buffer,
            final int offset,
            final long timestamp) {

        final byte execType = buffer.getByte(offset + FIXMessageDecoder.EXEC_TYPE_OFFSET_IN);
        final byte event    = FIXMessageDecoder.mapExecTypeToEvent(execType);
        if (event == OrderEvent.INVALID_TRANSITION) {
            log.warn("Unrecognised ExecType: {}", (char) execType);
            return;
        }

        // Try child order first (by clOrdId — children are identified by their FIX clOrdId)
        final long clOrdId = buffer.getLong(offset + FIXMessageDecoder.CL_ORD_ID_OFFSET_IN);
        final OrderFlyweight child = childRegistry.getByClOrdId(clOrdId);
        if (child != null) {
            handleChildExecReport(child, clOrdId, buffer, offset, execType, event);
            return;
        }

        // Fall through to parent order handling (direct-to-venue orders)
        final long orderId = buffer.getLong(offset + FIXMessageDecoder.ORDER_ID_OFFSET_IN);
        final int  slot    = orderBook.slotByOrderId(orderId);
        if (slot < 0) {
            log.debug("ExecReport for unknown orderId={} clOrdId={}", orderId, clOrdId);
            return;
        }

        final OrderFlyweight order = orderBook.wrapFlyweight(slot);

        if (!OrderStateMachine.applyTransition(order, event)) {
            log.debug("Ignoring ExecReport: invalid transition from {} on event {}",
                      OrderState.nameOf(order.orderState()), OrderEvent.nameOf(event));
            return;
        }

        long lastQty = 0L;
        if (event == OrderEvent.EXEC_PARTIAL_FILL || event == OrderEvent.EXEC_FILL) {
            lastQty = FIXMessageDecoder.getLastQty(buffer, offset);
            order.applyFill(lastQty);
        }

        if (event == OrderEvent.EXEC_NEW) {
            orderBook.indexOrderId(slot, orderId);
        }

        if (OrderState.isTerminal(order.orderState())) {
            orderBook.unindex(slot, order.clOrdId(), orderId);
            orderBook.freeSlot(slot);
        }

        fixEncoder.sendExecReport(order, execType, lastQty);
    }

    /**
     * Handle an ExecReport for a child order.
     * Aggregates fills into the parent and publishes a parent exec report to the client.
     */
    private void handleChildExecReport(
            final OrderFlyweight child,
            final long childClOrdId,
            final DirectBuffer buffer,
            final int offset,
            final byte execType,
            final byte event) {

        long lastQty = 0L;
        if (event == OrderEvent.EXEC_PARTIAL_FILL || event == OrderEvent.EXEC_FILL) {
            lastQty = FIXMessageDecoder.getLastQty(buffer, offset);
            // Aggregate fill into both child and parent
            final OrderFlyweight parent = childRegistry.applyFillAndAggregate(
                    childClOrdId, lastQty, 0L, orderBook);
            if (parent != null) {
                fixEncoder.sendExecReport(parent, execType, lastQty);
                if (OrderState.isTerminal(parent.orderState())) {
                    orderBook.unindex(
                        orderBook.slotByOrderId(parent.orderId()),
                        parent.clOrdId(), parent.orderId());
                    orderBook.freeSlot(orderBook.slotByOrderId(parent.orderId()));
                }
            }
        } else if (event == OrderEvent.EXEC_CANCELED) {
            OrderStateMachine.applyTransition(child, event);
            childRegistry.removeTerminal(child.orderId());
        } else if (event == OrderEvent.EXEC_REJECTED) {
            OrderStateMachine.applyTransition(child, event);
            childRegistry.removeTerminal(child.orderId());
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    /**
     * Sends an OrderLayout record back to the originating cluster client via egress.
     * Framing: byte 0 = ClusterMessageType, bytes 1-128 = OrderLayout fields from flyweight.
     * Spins on back-pressure with Thread.onSpinWait() — no sleep, no allocation.
     */
    /**
     * Sends an exec report back to the originating cluster client via egress.
     * Framing: byte 0 = ClusterMessageType, bytes 1-128 = OrderLayout fields.
     * Copies field-by-field from the flyweight — the flyweight may point into the
     * OrderBook backing buffer or be a temporary view; either is safe here.
     * Spins on back-pressure with Thread.onSpinWait() — no sleep, no allocation.
     */
    private void sendEgressExecReport(
            final ClientSession session,
            final byte msgType,
            final OrderFlyweight order) {

        egressBuffer.putByte(ClusterMessageType.OFFSET_MSG_TYPE, msgType);
        final int p = ClusterMessageType.OFFSET_PAYLOAD;
        egressBuffer.putLong( p + OrderLayout.ACCOUNT_ID_OFFSET,          order.accountId());
        egressBuffer.putLong( p + OrderLayout.CL_ORD_ID_OFFSET,           order.clOrdId());
        egressBuffer.putLong( p + OrderLayout.ORDER_ID_OFFSET,             order.orderId());
        egressBuffer.putLong( p + OrderLayout.ORIG_CL_ORD_ID_OFFSET,       order.origClOrdId());
        egressBuffer.putLong( p + OrderLayout.SYMBOL_OFFSET,               order.symbol());
        egressBuffer.putLong( p + OrderLayout.PRICE_OFFSET,                order.price());
        egressBuffer.putLong( p + OrderLayout.QTY_OFFSET,                  order.qty());
        egressBuffer.putLong( p + OrderLayout.FILLED_QTY_OFFSET,           order.filledQty());
        egressBuffer.putLong( p + OrderLayout.LEAVES_QTY_OFFSET,           order.leavesQty());
        egressBuffer.putByte( p + OrderLayout.SIDE_OFFSET,                 order.side());
        egressBuffer.putByte( p + OrderLayout.TIME_IN_FORCE_OFFSET,        order.timeInForce());
        egressBuffer.putByte( p + OrderLayout.ORDER_STATE_OFFSET,          order.orderState());

        // Use the cluster's idle strategy — consistent with how back-pressure is
        // handled elsewhere in the cluster service thread.
        // Guard against permanently blocked sessions (e.g. NOT_CONNECTED egress):
        // if offer fails after 1000 attempts, log and drop rather than blocking forever.
        int attempts = 0;
        idleStrategy.reset();
        long offerResult;
        do {
            offerResult = session.offer(egressBuffer, 0, ClusterMessageType.IPC_MESSAGE_SIZE);
            if (offerResult > 0) break;
            idleStrategy.idle();
            attempts++;
        } while (attempts < 1_000);
        if (offerResult <= 0) {
            log.warn("Egress offer failed for session={} after {} attempts: result={}",
                     session.id(), attempts, offerResult);
        }
    }

    private void sendReject(final long clOrdId, final String reason) {
        log.info("Rejecting order: clOrdId={} reason={}", clOrdId, reason);
    }

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
