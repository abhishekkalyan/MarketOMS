package com.cobain.oms.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * {@link ConfigSource} backed by a {@code .properties} file.
 *
 * <p>Property keys use the same names as the environment variables
 * (e.g. {@code OMS_MAX_ORDERS=65536}), so the same file doubles as a
 * drop-in replacement or complement to OS env vars.
 *
 * <p>The file path is supplied at construction time. The file is read once
 * eagerly in the constructor; any subsequent OS-level changes are not
 * reflected at runtime.
 *
 * <p>If the file does not exist or cannot be read, construction throws
 * {@link IllegalArgumentException} so the node fails fast at startup rather
 * than silently falling back to defaults.
 */
public final class PropertiesFileConfigSource implements ConfigSource {

    private static final Logger log = LoggerFactory.getLogger(PropertiesFileConfigSource.class);

    private final String     sourceName;
    private final Properties props;

    /**
     * @param filePath path to the {@code .properties} file (absolute or relative
     *                 to the process working directory)
     * @throws IllegalArgumentException if the file cannot be found or read
     */
    public PropertiesFileConfigSource(final String filePath) {
        this.sourceName = "file:" + filePath;
        this.props      = new Properties();
        try (InputStream in = new FileInputStream(filePath)) {
            props.load(in);
            log.info("Loaded config file: {} ({} entries)", filePath, props.size());
        } catch (final IOException e) {
            throw new IllegalArgumentException(
                    "Cannot read OMS config file '" + filePath + "': " + e.getMessage(), e);
        }
    }

    @Override
    public String name() {
        return sourceName;
    }

    @Override
    public String get(final String key) {
        final String v = props.getProperty(key);
        return (v != null && !v.isEmpty()) ? v : null;
    }
}
