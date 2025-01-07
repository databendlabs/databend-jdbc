package com.databend.client.data;

public abstract class ColumnTypeHandlerBase implements ColumnTypeHandler {
    protected boolean isNullable;

    public ColumnTypeHandlerBase(boolean isNullable) {
        this.isNullable = isNullable;
    }
    protected boolean isNull(String value) {
        return (value == null || value.equals("NULL"));
    }

    private boolean checkNull(String value){
        if (isNull(value)) {
            if (isNullable) {
                return true;
            } else {
                throw new IllegalArgumentException("type " + this.getClass().getName() + " is not nullable, but got " + value);
            }
        }
        return false;
    }

    @Override
    public Object parseValue(String value) {
        if (checkNull(value)) {
            return null;
        }
        return parseString(value);
    }

    @Override
    public void setNullable(boolean isNullable) {
        this.isNullable = isNullable;
    }

    abstract Object parseString(String value);
}
