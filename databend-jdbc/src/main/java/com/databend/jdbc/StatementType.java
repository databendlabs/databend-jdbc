package com.databend.jdbc;

enum StatementType {
    // SET
    PARAM_SETTING,
    // eg: SELECT, SHOW
    QUERY,
    // eg: INSERT
    NON_QUERY
}
