package com.cobain.oms.algo;

import com.cobain.oms.codec.ChildOrderIntentFlyweight;
import com.cobain.oms.model.OrderFlyweight;

/**
 * Zero-allocation sealed interface for algo execution engines.
 *
 * In the intent-driven architecture, engines are PURE COMPUTATION: they receive
 * a parent order, run their routing/slicing arithmetic, and publish
 * {@link ChildOrderIntentFlyweight} records via the {@link ChildIntentSink} callback.
 * They create NO order state. They touch NO order book. They dispatch NOTHING to the
 * FIX bridge.
 *
 * Sealed so that switch expressions in call-sites can be exhaustive at compile time
 * (Java 21 pattern-matching switch), and so the JIT can devirtualise the call.
 *
 * Implementations MUST NOT allocate on the hot path (onSlice / onTimer).
 * Object graphs are established once in the constructor; runtime state is held
 * in pre-allocated primitive fields.
 */
public sealed interface AlgoExecutionEngine
        permits IcebergAlgoEngine, TwapAlgoEngine {

    /**
     * Sink for computed child-order routing instructions.
     * The flyweight is owned by the engine; callers MUST NOT retain it past this call.
     */
    @FunctionalInterface
    interface ChildIntentSink {
        /**
         * Called for each computed child slice.
         *
         * @param intent  pre-allocated intent flyweight (do not retain reference)
         */
        void onIntent(ChildOrderIntentFlyweight intent);
    }

    /**
     * Compute and dispatch child order intents for a newly admitted parent order.
     *
     * @param parent     flyweight pointing at the validated parent order
     * @param sink       callback that receives each computed child intent
     * @param nowNanos   current wall-clock time in nanoseconds
     * @return           number of intents published to the sink
     */
    int onSlice(OrderFlyweight parent, ChildIntentSink sink, long nowNanos);

    /**
     * Called when a cluster timer fires (Aeron Cluster onTimerEvent).
     * TWAP uses this to dispatch the next scheduled slice.
     * Iceberg ignores timer events.
     *
     * @param correlationId the correlation ID registered with Cluster.scheduleTimer
     * @param timestamp     current cluster timestamp in milliseconds
     */
    void onTimer(long correlationId, long timestamp);

    /**
     * Reset all in-flight algo state.
     * Called during snapshot restore before replaying persisted algo state.
     */
    void reset();
}
