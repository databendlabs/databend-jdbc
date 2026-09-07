package com.databend.jdbc.internal;

import com.databend.jdbc.internal.exception.DatabendQueryException;

/**
 * Arrow decoding needs Java 11 bytecode, but the driver still supports Java 8 for JSON results.
 * This class only inspects a version string and must never reference Arrow types, so it stays
 * loadable on Java 8. Do not probe Arrow classes to detect support: that is the linkage error
 * this check exists to avoid.
 */
public final class ArrowRuntime {
    public static final String UNSUPPORTED_MESSAGE =
            "Arrow result format requires Java 11 or newer; use query_result_format=json";

    private static final int MIN_JAVA_VERSION = 11;

    private ArrowRuntime() {
    }

    public static boolean isSupported() {
        return isSupported(System.getProperty("java.specification.version"));
    }

    public static boolean isSupported(String version) {
        int major;
        try {
            major = Integer.parseInt(version != null && version.startsWith("1.") ? version.substring(2) : version);
        } catch (NumberFormatException e) {
            // Unknown runtime: leave compatibility checks to the JVM rather than failing to parse a property.
            return true;
        }
        return major >= MIN_JAVA_VERSION;
    }

    public static void requireSupported() {
        if (!isSupported()) {
            throw new DatabendQueryException(UNSUPPORTED_MESSAGE);
        }
    }
}
