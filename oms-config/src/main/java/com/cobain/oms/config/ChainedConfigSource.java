package com.cobain.oms.config;

/**
 * {@link ConfigSource} that delegates to an ordered list of sources,
 * returning the first non-null value found.
 *
 * <p>Convention: list sources in descending priority order — the first
 * source that defines a key wins. Typical usage:
 * <pre>
 *   new ChainedConfigSource(fileSource, EnvVarConfigSource.INSTANCE)
 * </pre>
 * This gives file values priority over environment variables. When neither
 * defines a key, {@link OmsConfig#load} applies the hard-coded default.
 *
 * <p>{@link #sourceName(String)} walks the chain and returns the name of
 * the first source that actually holds the key, or {@code "default"} when
 * no source defines it.
 */
public final class ChainedConfigSource implements ConfigSource {

    private final ConfigSource[] sources;

    public ChainedConfigSource(final ConfigSource... sources) {
        if (sources == null || sources.length == 0) {
            throw new IllegalArgumentException("ChainedConfigSource requires at least one source");
        }
        this.sources = sources.clone();
    }

    @Override
    public String name() {
        return "chain";
    }

    @Override
    public String get(final String key) {
        for (final ConfigSource source : sources) {
            final String value = source.get(key);
            if (value != null) {
                return value;
            }
        }
        return null;
    }

    /**
     * Returns the {@link ConfigSource#name()} of the first source in the chain
     * that defines {@code key}, or {@code "default"} if none do.
     */
    @Override
    public String sourceName(final String key) {
        for (final ConfigSource source : sources) {
            if (source.get(key) != null) {
                return source.name();
            }
        }
        return "default";
    }
}
