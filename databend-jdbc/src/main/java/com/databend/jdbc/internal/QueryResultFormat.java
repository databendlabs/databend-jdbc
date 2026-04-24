package com.databend.jdbc.internal;

import java.util.Locale;

public enum QueryResultFormat {
    JSON,
    ARROW;

    public static QueryResultFormat fromValue(String value) {
        return QueryResultFormat.valueOf(value.trim().toUpperCase(Locale.ENGLISH));
    }

    public String value() {
        return name().toLowerCase(Locale.ENGLISH);
    }
}
