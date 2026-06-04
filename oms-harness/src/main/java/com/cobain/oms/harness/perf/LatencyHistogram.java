package com.cobain.oms.harness.perf;

import java.io.PrintStream;

/**
 * Zero-allocation histogram for recording latency values in nanoseconds.
 *
 * <h2>Implementation choice</h2>
 * Backed by a pre-allocated {@code long[]} array of {@code bucketCount} buckets.
 * Each bucket covers a range of {@code bucketWidthNs} nanoseconds. Values beyond
 * the maximum bucket are clamped to the last bucket (overflow tracked separately).
 * HdrHistogram was rejected to avoid an external dependency; the record() path here
 * is provably zero-allocation (array index write only).
 *
 * <h2>Zero-GC contract</h2>
 * <ul>
 *   <li>{@link #record(long)} — ZERO allocation: one array bounds check + one array write.</li>
 *   <li>{@link #percentile(double)} — ZERO allocation: scan of primitive long array.</li>
 *   <li>{@link #max()} — ZERO allocation.</li>
 *   <li>{@link #reset()} — ZERO allocation: {@code Arrays.fill} on primitive array.</li>
 *   <li>{@link #print(PrintStream)} — allocates: creates char[] for number formatting.
 *       Only called off hot path for reporting.</li>
 * </ul>
 */
public final class LatencyHistogram {

    private static final int    DEFAULT_BUCKET_COUNT    = 100_000;
    private static final long   DEFAULT_BUCKET_WIDTH_NS = 1_000L;   // 1 µs per bucket → max 100 ms

    private final long[] buckets;
    private final long   bucketWidthNs;
    private final int    bucketCount;

    private long totalCount;
    private long maxValueNs;

    /** Constructs with default 100 000 buckets of 1 µs each (covers 0–100 ms). */
    public LatencyHistogram() {
        this(DEFAULT_BUCKET_COUNT, DEFAULT_BUCKET_WIDTH_NS);
    }

    /**
     * @param bucketCount   number of buckets (pre-allocated)
     * @param bucketWidthNs width of each bucket in nanoseconds
     */
    public LatencyHistogram(final int bucketCount, final long bucketWidthNs) {
        this.bucketCount   = bucketCount;
        this.bucketWidthNs = bucketWidthNs;
        this.buckets       = new long[bucketCount];
    }

    /**
     * Records one latency observation. ZERO allocation on this path.
     *
     * @param valueNanos latency in nanoseconds; negative values are clamped to 0
     */
    public void record(final long valueNanos) {
        final long v = valueNanos < 0 ? 0 : valueNanos;
        int idx = (int) (v / bucketWidthNs);
        if (idx >= bucketCount) {
            idx = bucketCount - 1;
        }
        buckets[idx]++;
        totalCount++;
        if (v > maxValueNs) {
            maxValueNs = v;
        }
    }

    /**
     * Returns the value (in nanoseconds) at the given percentile. ZERO allocation.
     *
     * @param pct percentile in range [0.0, 100.0]
     * @return nanoseconds at that percentile, or 0 if no samples recorded
     */
    public long percentile(final double pct) {
        if (totalCount == 0) return 0L;
        final long target = (long) Math.ceil(pct / 100.0 * totalCount);
        long cumulative = 0L;
        for (int i = 0; i < bucketCount; i++) {
            cumulative += buckets[i];
            if (cumulative >= target) {
                return (long) i * bucketWidthNs + bucketWidthNs / 2;
            }
        }
        return (long) (bucketCount - 1) * bucketWidthNs;
    }

    /** Returns the maximum recorded value in nanoseconds. ZERO allocation. */
    public long max() {
        return maxValueNs;
    }

    /** Returns the total number of recorded observations. ZERO allocation. */
    public long totalCount() {
        return totalCount;
    }

    /** Clears all recorded data. ZERO allocation. */
    public void reset() {
        java.util.Arrays.fill(buckets, 0L);
        totalCount  = 0L;
        maxValueNs  = 0L;
    }

    /**
     * Prints a summary with p50/p90/p99/p99.9/p99.99/max in microseconds and
     * an ASCII bar chart of the distribution. Allocates (number formatting).
     * Not called on the hot path.
     */
    public void print(final PrintStream out) {
        out.println("  Latency distribution (µs):");
        out.println("    p50    = " + (percentile(50.0) / 1_000) + " µs");
        out.println("    p90    = " + (percentile(90.0) / 1_000) + " µs");
        out.println("    p99    = " + (percentile(99.0) / 1_000) + " µs");
        out.println("    p99.9  = " + (percentile(99.9) / 1_000) + " µs");
        out.println("    p99.99 = " + (percentile(99.99) / 1_000) + " µs");
        out.println("    max    = " + (maxValueNs / 1_000) + " µs");
        out.println("    count  = " + totalCount);

        // ASCII bar chart — find the first and last non-empty bucket
        int firstNonEmpty = -1;
        int lastNonEmpty  = -1;
        for (int i = 0; i < bucketCount; i++) {
            if (buckets[i] > 0) {
                if (firstNonEmpty == -1) firstNonEmpty = i;
                lastNonEmpty = i;
            }
        }
        if (firstNonEmpty == -1) return;

        // Find max bucket count for scaling
        long maxBucket = 0;
        for (int i = firstNonEmpty; i <= lastNonEmpty; i++) {
            if (buckets[i] > maxBucket) maxBucket = buckets[i];
        }

        final int BAR_WIDTH = 40;
        out.println("  Histogram (each row = " + bucketWidthNs / 1_000 + " µs bucket):");
        // Print at most 40 rows by aggregating buckets if range is large
        final int range = lastNonEmpty - firstNonEmpty + 1;
        final int step  = Math.max(1, range / BAR_WIDTH);
        for (int i = firstNonEmpty; i <= lastNonEmpty; i += step) {
            long count = 0;
            for (int j = i; j < Math.min(i + step, bucketCount); j++) {
                count += buckets[j];
            }
            final long bucketStartUs = (long) i * bucketWidthNs / 1_000;
            final int  barLen        = (int) (count * BAR_WIDTH / maxBucket);
            out.print("    " + pad(bucketStartUs, 6) + " µs |");
            for (int b = 0; b < barLen; b++) out.print('#');
            out.println(" " + count);
        }
    }

    private static String pad(final long value, final int width) {
        final String s = Long.toString(value);
        if (s.length() >= width) return s;
        final StringBuilder sb = new StringBuilder(width);
        for (int i = s.length(); i < width; i++) sb.append(' ');
        sb.append(s);
        return sb.toString();
    }
}
