package com.cobain.oms.harness;

import com.cobain.oms.model.OrderLayout;
import com.cobain.oms.model.OrderState;
import com.cobain.oms.model.Side;
import com.cobain.oms.model.TimeInForce;
import io.aeron.cluster.client.AeronCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Drives end-to-end test scenarios against a running OmsNode.
 *
 * Each scenario method injects one or more orders via OrderInjector, then
 * uses ExecReportListener.awaitState() to assert the expected terminal state.
 *
 * Returns true if all assertions pass, false otherwise.
 * All results are logged — no exception thrown on assertion failure so the
 * harness can run all scenarios and report a complete pass/fail summary.
 */
public final class ScenarioRunner {

    private static final Logger log = LoggerFactory.getLogger(ScenarioRunner.class);

    private static final long ACCOUNT_ID   = 42_001L;
    private static final long TIMEOUT_MS   = 5_000L;   // 5 seconds per assertion

    private final AeronCluster      cluster;
    private final OrderInjector     injector;
    private final ExecReportListener listener;

    private int passed = 0;
    private int failed = 0;

    public ScenarioRunner(
            final AeronCluster cluster,
            final OrderInjector injector,
            final ExecReportListener listener) {
        this.cluster  = cluster;
        this.injector = injector;
        this.listener = listener;
    }

    // ── Scenario 1: Single order — new → routed → partially filled → filled ──

    /**
     * Injects a BUY AAPL order and expects it to reach NEW state (accepted by oms-core).
     * Full fill simulation requires the venue stub to send exec reports back, which
     * in the local harness means we assert NEW as the terminal acceptance state.
     */
    public boolean scenarioSingleOrderAccepted() {
        log.info("── Scenario 1: Single order accepted ────────────────────────");
        final long price = 150_0000L;  // £150.0000 in fixed-point (x10000)
        final long qty   = 1_000L;

        final long clOrdId = injector.inject(
                ACCOUNT_ID, "AAPL", Side.BUY, price, qty, TimeInForce.DAY);

        final boolean ok = awaitAndAssert(clOrdId, OrderState.NEW, "BUY AAPL 1000@150 accepted");
        record("Scenario 1 - SingleOrderAccepted", ok);
        return ok;
    }

    // ── Scenario 2: Order rejected by validation (invalid qty) ───────────────

    /**
     * Injects an order with qty=0 — should be rejected by OrderValidationEngine.
     */
    public boolean scenarioRejectedInvalidQty() {
        log.info("── Scenario 2: Order rejected — zero qty ────────────────────");
        final long clOrdId = injector.inject(
                ACCOUNT_ID, "MSFT", Side.BUY,
                300_0000L, // price
                0L,        // qty = 0 → invalid
                TimeInForce.DAY);

        final boolean ok = awaitAndAssert(clOrdId, OrderState.REJECTED, "zero-qty order rejected");
        record("Scenario 2 - RejectedInvalidQty", ok);
        return ok;
    }

    // ── Scenario 3: Order rejected — unknown symbol ───────────────────────────

    /**
     * Injects an order with a symbol not in OMS_SYMBOLS — should be rejected.
     */
    public boolean scenarioRejectedUnknownSymbol() {
        log.info("── Scenario 3: Order rejected — unknown symbol ───────────────");
        final long clOrdId = injector.inject(
                ACCOUNT_ID, "ZZZZ", Side.SELL,
                50_0000L, 500L, TimeInForce.DAY);

        final boolean ok = awaitAndAssert(clOrdId, OrderState.REJECTED, "unknown symbol rejected");
        record("Scenario 3 - RejectedUnknownSymbol", ok);
        return ok;
    }

    // ── Scenario 4: Cancel request ────────────────────────────────────────────

    /**
     * Injects a BUY GOOG order, waits for NEW, then sends a cancel request.
     * Expects the order to reach PENDING_CANCEL (venue ack not simulated locally).
     */
    public boolean scenarioCancelRequest() throws InterruptedException {
        log.info("── Scenario 4: Cancel request ───────────────────────────────");
        final long clOrdId = injector.inject(
                ACCOUNT_ID, "GOOG", Side.BUY,
                140_0000L, 200L, TimeInForce.GTC);

        // Wait for order to be accepted first
        if (!awaitState(clOrdId, OrderState.NEW, "GOOG order accepted before cancel")) {
            record("Scenario 4 - CancelRequest", false);
            return false;
        }

        // Send cancel
        injector.injectCancel(ACCOUNT_ID, clOrdId, "GOOG");

        final boolean ok = awaitAndAssert(clOrdId, OrderState.PENDING_CANCEL, "cancel request sent");
        record("Scenario 4 - CancelRequest", ok);
        return ok;
    }

    // ── Scenario 5: Bulk injection — stress test ─────────────────────────────

    /**
     * Injects 50 orders across AAPL, MSFT, AMZN and asserts all reach NEW.
     * Validates the OMS can handle a burst without dropping messages.
     */
    public boolean scenarioBulkInjection() throws InterruptedException {
        log.info("── Scenario 5: Bulk injection (50 orders) ───────────────────");
        final String[] symbols = {"AAPL", "MSFT", "AMZN"};
        final long[]   prices  = {150_0000L, 300_0000L, 180_0000L};
        final long[]   clOrdIds = new long[50];

        for (int i = 0; i < 50; i++) {
            final int s = i % symbols.length;
            clOrdIds[i] = injector.inject(
                    ACCOUNT_ID, symbols[s], (i % 2 == 0) ? Side.BUY : Side.SELL,
                    prices[s], 100L + i, TimeInForce.DAY);
        }

        int localPassed = 0;
        for (final long clOrdId : clOrdIds) {
            if (awaitState(clOrdId, OrderState.NEW, null)) localPassed++;
        }
        final boolean ok = (localPassed == clOrdIds.length);
        log.info("Bulk injection: {}/{} orders reached NEW state", localPassed, clOrdIds.length);
        record("Scenario 5 - BulkInjection", ok);
        return ok;
    }

    // ── Summary ───────────────────────────────────────────────────────────────

    public void printSummary() {
        log.info("");
        log.info("════════════════════════════════════════════════════════");
        log.info("  HARNESS RESULTS:  {} passed,  {} failed", passed, failed);
        log.info("  Exec reports received: {}", listener.totalReceived());
        log.info("  Filled: {}  Canceled: {}  Rejected: {}",
                listener.totalFilled(), listener.totalCanceled(), listener.totalRejected());
        log.info("════════════════════════════════════════════════════════");
    }

    public boolean allPassed() { return failed == 0; }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private boolean awaitAndAssert(final long clOrdId, final byte expectedState, final String desc) {
        try {
            final boolean reached =
                    listener.awaitState(cluster, clOrdId, expectedState, TIMEOUT_MS);
            if (reached) {
                log.info("  PASS: {} — clOrdId={} reached state={}",
                        desc, clOrdId, OrderState.nameOf(expectedState));
            } else {
                log.error("  FAIL: {} — clOrdId={} did not reach state={} within {}ms",
                        desc, clOrdId, OrderState.nameOf(expectedState), TIMEOUT_MS);
            }
            return reached;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private boolean awaitState(final long clOrdId, final byte expectedState, final String desc)
            throws InterruptedException {
        final boolean reached =
                listener.awaitState(cluster, clOrdId, expectedState, TIMEOUT_MS);
        if (desc != null && !reached) {
            log.error("  FAIL: {} — clOrdId={} did not reach state={}",
                    desc, clOrdId, OrderState.nameOf(expectedState));
        }
        return reached;
    }

    private void record(final String scenarioName, final boolean ok) {
        if (ok) passed++; else failed++;
        log.info("  [{}] {}", ok ? "PASS" : "FAIL", scenarioName);
    }
}
