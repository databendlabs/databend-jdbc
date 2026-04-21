package com.databend.jdbc.internal.data;

import java.math.BigDecimal;
import java.math.BigInteger;

class Int8Handler extends ColumnTypeHandlerBase {
    public Int8Handler(boolean isNullable) {
        super(isNullable);
    }

    @Override
    public Object parseStringNotNull(String value) {
        return Byte.parseByte(value);
    }
}

class Int16Handler extends ColumnTypeHandlerBase {
    public Int16Handler(boolean isNullable) {
        super(isNullable);
    }

    @Override
    public Object parseStringNotNull(String value) {
        return Short.parseShort(value);
    }
}

class Int32Handler extends ColumnTypeHandlerBase {
    public Int32Handler(boolean isNullable) {
        super(isNullable);
    }

    @Override
    public Object parseStringNotNull(String value) {
        return Integer.parseInt(value);
    }
}

class Int64Handler extends ColumnTypeHandlerBase {
    public Int64Handler(boolean isNullable) {
        super(isNullable);
    }

    @Override
    public Object parseStringNotNull(String value) {
        return Long.parseLong(value);
    }
}

class UInt8Handler extends ColumnTypeHandlerBase {
    public UInt8Handler(boolean isNullable) {
        super(isNullable);
    }

    @Override
    public Object parseStringNotNull(String value) {
        return Short.parseShort(value);
    }
}

class UInt16Handler extends ColumnTypeHandlerBase {
    public UInt16Handler(boolean isNullable) {
        super(isNullable);
    }

    @Override
    public Object parseStringNotNull(String value) {
        return Integer.parseInt(value);
    }
}

class UInt32Handler extends ColumnTypeHandlerBase {
    public UInt32Handler(boolean isNullable) {
        super(isNullable);
    }

    @Override
    public Object parseStringNotNull(String value) {
        return Long.parseLong(value);
    }
}

class UInt64Handler extends ColumnTypeHandlerBase {
    public UInt64Handler(boolean isNullable) {
        super(isNullable);
    }

    @Override
    public Object parseStringNotNull(String value) {
        return new BigInteger(value);
    }
}

class Float32Handler extends ColumnTypeHandlerBase {
    public Float32Handler(boolean isNullable) {
        super(isNullable);
    }

    @Override
    public Object parseStringNotNull(String value) {
        if ("NaN".equals(value) || "nan".equals(value)) {
            return Double.NaN;
        }
        if ("Infinity".equals(value) || "inf".equals(value)) {
            return Double.POSITIVE_INFINITY;
        }
        return Float.parseFloat(value);
    }
}

class Float64Handler extends ColumnTypeHandlerBase {
    public Float64Handler(boolean isNullable) {
        super(isNullable);
    }

    @Override
    public Object parseStringNotNull(String value) {
        if ("NaN".equals(value) || "nan".equals(value)) {
            return Double.NaN;
        }
        if ("Infinity".equals(value) || "inf".equals(value)) {
            return Double.POSITIVE_INFINITY;
        }
        return Double.parseDouble(value);
    }
}

class BooleanHandler extends ColumnTypeHandlerBase {
    private static final String TRUE_NUM = "1";
    private static final String TRUE_STRING = "true";

    public BooleanHandler(boolean isNullable) {
        super(isNullable);
    }

    @Override
    public Object parseStringNotNull(String value) {
        return TRUE_NUM.equals(value) || TRUE_STRING.equals(value);
    }
}

class DecimalHandler extends ColumnTypeHandlerBase {
    public DecimalHandler(boolean isNullable) {
        super(isNullable);
    }

    @Override
    public Object parseStringNotNull(String value) {
        return new BigDecimal(value);
    }
}
