package com.cobain.oms.algo;

import com.cobain.oms.model.OrderFlyweight;

/**
 * Zero-allocation sealed interface for algo execution engines.
 *
 * Sealed so that the switch expressions in OmsClusteredService can be exhaustive
 * at compile time (Java 21 pattern-matching switch), and so the JIT can devirtualise
 * the call in the common single-algo case.
 *
 * Implementations MUST NOT allocate on the hot path (onOrder / onTimer).
 * Object graphs are established once in the constructor; runtime state is held
 * in pre-allocated primitive arrays.
 */
public sealed interface AlgoExecutionEngine
        permits IcebergAlgoEngine, TwapAlgoEngine {

    /**
     * Process a validated, newly admitted parent order.
     * Implementations should immediately dispatch child slices (Iceberg) or
     * register a timer with the cluster (TWAP).
     *
     * @param parentOrder flyweight pointing to the order in the OrderBook backing array
     * @param parentSlot  slot index — passed explicitly so implementations can track without
     *                    a secondary map lookup
     * @param timestamp   cluster-monotonic timestamp in milliseconds
     */
    void onOrder(OrderFlyweight parentOrder, int parentSlot, long timestamp);

    /**
     * Called when a cluster timer fires (Aeron Cluster {@code onTimerEvent}).
     * TWAP uses this to send the next scheduled slice.
     * Iceberg ignores timer events (it reacts to fill events instead).
     *
     * @param correlationId the correlation ID registered with {@code Cluster.scheduleTimer}
     * @param timestamp     current cluster timestamp in milliseconds
     */
    void onTimer(long correlationId, long timestamp);

    /**
     * Called when an ExecReport arrives for a child order.
     * Iceberg uses this to decide whether to expose the next peak.
     *
     * @param childSlot slot index of the child order in the OrderBook
     * @param lastQty   quantity filled in this execution event
     */
    void onChildFill(int childSlot, long lastQty);

    /**
     * Reset all in-flight algo state.
     * Called during snapshot restore before replaying persisted algo state.
     */
    void reset();
}
