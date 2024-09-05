package com.databend.jdbc;

public enum StatementType {
    PARAM_SETTING, // SET
    QUERY, // eg: SELECT, SHOW
    NON_QUERY // eg: INSERT
}
