package com.cobain.oms.config;

/**
 * Pluggable source of raw string configuration values.
 *
 * Implementations: {@link EnvVarConfigSource}, {@link PropertiesFileConfigSource},
 * {@link ChainedConfigSource}.
 *
 * The interface is intentionally minimal — callers always go through {@link OmsConfig#load}
 * which handles parsing, validation, defaults, and startup logging.
 */
public interface ConfigSource {

    /** Human-readable name used in startup log lines (e.g. {@code "env"} or {@code "file:oms.properties"}). */
    String name();

    /**
     * Returns the raw string value for {@code key}, or {@code null} if this source
     * does not define it.
     */
    String get(String key);

    /**
     * Returns the name of the source that actually supplies {@code key}, or
     * {@code "default"} when this source does not define it.
     *
     * <p>Overridden by {@link ChainedConfigSource} to walk the chain and report
     * the first matching source.
     */
    default String sourceName(final String key) {
        return get(key) != null ? name() : "default";
    }
}
