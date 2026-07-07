package com.databend.jdbc.internal.binding;

public enum StatementType {
    // eg: SELECT, SHOW
    QUERY,
    // eg: INSERT
    NON_QUERY
}
