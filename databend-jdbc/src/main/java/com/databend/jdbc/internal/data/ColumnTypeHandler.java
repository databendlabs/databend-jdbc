package com.databend.jdbc.internal.data;

public interface ColumnTypeHandler {
    Object parseValue(Object value) throws IllegalArgumentException;

    Object parseString(String value) throws IllegalArgumentException;

    void setNullable(boolean isNullable);
}
