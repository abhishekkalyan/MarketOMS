package com.cobain.oms.config;

/**
 * {@link ConfigSource} backed by OS environment variables.
 *
 * Key names are used verbatim (e.g. {@code "OMS_MAX_ORDERS"}).
 * Returns {@code null} for any key that is absent or set to an empty string.
 */
public final class EnvVarConfigSource implements ConfigSource {

    public static final EnvVarConfigSource INSTANCE = new EnvVarConfigSource();

    @Override
    public String name() {
        return "env";
    }

    @Override
    public String get(final String key) {
        final String v = System.getenv(key);
        return (v != null && !v.isEmpty()) ? v : null;
    }
}
