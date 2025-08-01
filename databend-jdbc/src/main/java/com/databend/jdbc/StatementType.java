package com.databend.jdbc;

public enum StatementType {
    // SET
    PARAM_SETTING,
    // eg: SELECT, SHOW
    QUERY,
    // eg: INSERT
    NON_QUERY
}
