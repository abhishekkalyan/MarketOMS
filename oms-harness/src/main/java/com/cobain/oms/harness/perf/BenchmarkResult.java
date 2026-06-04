package com.cobain.oms.harness.perf;

import java.util.Map;

/**
 * Result from a single benchmark run.
 * The metrics map is allocated once per result, off the hot path — acceptable.
 */
public final class BenchmarkResult {

    public final String              name;
    public final boolean             passed;
    public final Map<String, String> metrics;

    public BenchmarkResult(
            final String name,
            final boolean passed,
            final Map<String, String> metrics) {
        this.name    = name;
        this.passed  = passed;
        this.metrics = metrics;
    }
}
