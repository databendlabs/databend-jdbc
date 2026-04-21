package com.databend.jdbc.internal.data;

class StringHandler extends ColumnTypeHandlerBase {
    public StringHandler(boolean isNullable) {
        super(isNullable);
    }

    @Override
    protected boolean isNull(String value) {
        return value == null;
    }

    @Override
    public Object parseStringNotNull(String value) {
        return value;
    }
}
