/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.databend.client.data;

import java.math.BigDecimal;
import java.math.BigInteger;

class Int8Handler implements ColumnTypeHandler {
    private boolean isNullable;

    public Int8Handler() {
        this.isNullable = false;
    }

    public Int8Handler(boolean isNullable) {
        this.isNullable = isNullable;
    }

    @Override
    public Object parseValue(Object value) {
        if (isNullable) {
            return parseNullableValue(value);
        } else {
            return parseNonNullValue(value);

        }
    }

    private Byte parseNullableValue(Object value) {
        if (value == null || value.equals("NULL")) {
            return null;
        }
        if (value instanceof String) {
            return Byte.parseByte((String) value);
        }
        if (value instanceof Number) {
            return ((Number) value).byteValue();
        }
        return null;
    }

    private byte parseNonNullValue(Object value) {
        if (value == null || value.equals("NULL")) {
            throw new IllegalArgumentException("Int8 type is not nullable");
        }
        if (value instanceof String) {
            return Byte.parseByte((String) value);
        }
        if (value instanceof Number) {
            return ((Number) value).byteValue();
        }
        return 0;
    }

    @Override
    public void setNullable(boolean isNullable) {
        this.isNullable = isNullable;
    }
}

class Int16Handler implements ColumnTypeHandler {
    private boolean isNullable;

    public Int16Handler() {
        this.isNullable = false;
    }

    public Int16Handler(boolean isNullable) {
        this.isNullable = isNullable;
    }

    @Override
    public Object parseValue(Object value) {
        if (isNullable) {
            return parseNullableValue(value);
        } else {
            return parseNonNullValue(value);
        }
    }

    private Short parseNullableValue(Object value) {
        if (value == null || value.equals("NULL")) {
            return null;
        }
        if (value instanceof String) {
            return Short.parseShort((String) value);
        }
        if (value instanceof Number) {
            return ((Number) value).shortValue();
        }
        return null;
    }

    private short parseNonNullValue(Object value) {
        if (value == null || value.equals("NULL")) {
            throw new IllegalArgumentException("Int16 type is not nullable");
        }
        if (value instanceof String) {
            return Short.parseShort((String) value);
        }
        if (value instanceof Number) {
            return ((Number) value).shortValue();
        }
        return 0;
    }

    @Override
    public void setNullable(boolean isNullable) {
        this.isNullable = isNullable;
    }
}

// int32
class Int32Handler implements ColumnTypeHandler {
    private boolean isNullable;

    public Int32Handler() {
        this.isNullable = false;
    }

    public Int32Handler(boolean isNullable) {
        this.isNullable = isNullable;
    }

    @Override
    public Object parseValue(Object value) {
        if (isNullable) {
            return parseNullableValue(value);
        } else {
            return parseNonNullValue(value);
        }
    }

    private Integer parseNullableValue(Object value) {
        if (value == null || value.equals("NULL")) {
            return null;
        }
        if (value instanceof String) {
            return Integer.parseInt((String) value);
        }
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        return null;
    }

    private int parseNonNullValue(Object value) {
        if (value == null || value.equals("NULL")) {
            throw new IllegalArgumentException("Int32 type is not nullable");
        }
        if (value instanceof String) {
            return Integer.parseInt((String) value);
        }
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        return 0;
    }

    @Override
    public void setNullable(boolean isNullable) {
        this.isNullable = isNullable;
    }
}

// int64
class Int64Handler implements ColumnTypeHandler {
    private boolean isNullable;

    public Int64Handler() {
        this.isNullable = false;
    }

    public Int64Handler(boolean isNullable) {
        this.isNullable = isNullable;
    }

    @Override
    public Object parseValue(Object value) {
        if (isNullable) {
            return parseNullableValue(value);
        } else {
            return parseNonNullValue(value);
        }
    }

    private Long parseNullableValue(Object value) {
        if (value == null || value.equals("NULL")) {
            return null;
        }
        if (value instanceof String) {
            return Long.parseLong((String) value);
        }
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        return null;
    }

    private long parseNonNullValue(Object value) {
        if (value == null || value.equals("NULL")) {
            throw new IllegalArgumentException("Int64 type is not nullable");
        }
        if (value instanceof String) {
            return Long.parseLong((String) value);
        }
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        return 0;
    }

    @Override
    public void setNullable(boolean isNullable) {
        this.isNullable = isNullable;
    }
}

// uint8
class UInt8Handler implements ColumnTypeHandler {
    private boolean isNullable;

    public UInt8Handler() {
        this.isNullable = false;
    }

    public UInt8Handler(boolean isNullable) {
        this.isNullable = isNullable;
    }

    @Override
    public Object parseValue(Object value) {
        if (isNullable) {
            return parseNullableValue(value);
        } else {
            return parseNonNullValue(value);
        }
    }

    private Short parseNullableValue(Object value) {
        if (value == null || value.equals("NULL")) {
            return null;
        }
        if (value instanceof String) {
            return Short.parseShort((String) value);
        }
        if (value instanceof Number) {
            return ((Number) value).shortValue();
        }
        return null;
    }

    private short parseNonNullValue(Object value) {
        if (value == null || value.equals("NULL")) {
            throw new IllegalArgumentException("UInt8 type is not nullable");
        }
        if (value instanceof String) {
            return Short.parseShort((String) value);
        }
        if (value instanceof Number) {
            return ((Number) value).shortValue();
        }
        return 0;
    }

    @Override
    public void setNullable(boolean isNullable) {
        this.isNullable = isNullable;
    }
}

// uint16
class UInt16Handler implements ColumnTypeHandler {
    private boolean isNullable;

    public UInt16Handler() {
        this.isNullable = false;
    }

    public UInt16Handler(boolean isNullable) {
        this.isNullable = isNullable;
    }

    @Override
    public Object parseValue(Object value) {
        if (isNullable) {
            return parseNullableValue(value);
        } else {
            return parseNonNullValue(value);
        }
    }

    private Integer parseNullableValue(Object value) {
        if (value == null || value.equals("NULL")) {
            return null;
        }
        if (value instanceof String) {
            return Integer.parseInt((String) value);
        }
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        return null;
    }

    private int parseNonNullValue(Object value) {
        if (value == null || value.equals("NULL")) {
            throw new IllegalArgumentException("UInt16 type is not nullable");
        }
        if (value instanceof String) {
            return Integer.parseInt((String) value);
        }
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        return 0;
    }

    @Override
    public void setNullable(boolean isNullable) {
        this.isNullable = isNullable;
    }
}

// uint32
class UInt32Handler implements ColumnTypeHandler {
    private boolean isNullable;

    public UInt32Handler() {
        this.isNullable = false;
    }

    public UInt32Handler(boolean isNullable) {
        this.isNullable = isNullable;
    }

    @Override
    public Object parseValue(Object value) {
        if (isNullable) {
            return parseNullableValue(value);
        } else {
            return parseNonNullValue(value);
        }
    }

    private Long parseNullableValue(Object value) {
        if (value == null || value.equals("NULL")) {
            return null;
        }
        if (value instanceof String) {
            return Long.parseLong((String) value);
        }
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        return null;
    }

    private long parseNonNullValue(Object value) {
        if (value == null || value.equals("NULL")) {
            throw new IllegalArgumentException("UInt32 type is not nullable");
        }
        if (value instanceof String) {
            return Long.parseLong((String) value);
        }
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        return 0;
    }

    @Override
    public void setNullable(boolean isNullable) {
        this.isNullable = isNullable;
    }
}

// uint64
class UInt64Handler implements ColumnTypeHandler {
    private boolean isNullable;

    public UInt64Handler() {
        this.isNullable = false;
    }

    public UInt64Handler(boolean isNullable) {
        this.isNullable = isNullable;
    }

    @Override
    public Object parseValue(Object value) {
        if (isNullable) {
            return parseNullableValue(value);
        } else {
            return parseNonNullValue(value);
        }
    }

    private BigInteger parseNullableValue(Object value) {
        if (value == null || value.equals("NULL")) {
            return null;
        }
        if (value instanceof String) {
            return new BigInteger((String) value);
        }
        if (value instanceof Number) {
            return BigInteger.valueOf(((Number) value).longValue());
        }
        return null;
    }

    private BigInteger parseNonNullValue(Object value) {
        if (value == null || value.equals("NULL")) {
            throw new IllegalArgumentException("UInt64 type is not nullable");
        }
        if (value instanceof String) {
            return new BigInteger((String) value);
        }
        if (value instanceof Number) {
            return BigInteger.valueOf(((Number) value).longValue());
        }
        return BigInteger.ZERO;
    }

    @Override
    public void setNullable(boolean isNullable) {
        this.isNullable = isNullable;
    }
}

// float32
class Float32Handler implements ColumnTypeHandler {
    private boolean isNullable;

    public Float32Handler() {
        this.isNullable = false;
    }

    public Float32Handler(boolean isNullable) {
        this.isNullable = isNullable;
    }

    @Override
    public Object parseValue(Object value) {
        if (isNullable) {
            return parseNullableValue(value);
        } else {
            return parseNonNullValue(value);
        }
    }

    private Float parseNullableValue(Object value) {
        if (value == null || value.equals("NULL")) {
            return null;
        }
        if (value.equals("NaN") || value.equals("nan")) {
            return Float.NaN;
        }
        if (value instanceof String) {
            return Float.parseFloat((String) value);
        }
        if (value instanceof Number) {
            return ((Number) value).floatValue();
        }
        return null;
    }

    private float parseNonNullValue(Object value) {
        if (value == null || value.equals("NULL")) {
            throw new IllegalArgumentException("Float32 type is not nullable");
        }
        if (value.equals("NaN") || value.equals("nan")) {
            return Float.NaN;
        }
        if (value instanceof String) {
            return Float.parseFloat((String) value);
        }
        if (value instanceof Number) {
            return ((Number) value).floatValue();
        }
        return 0;
    }

    @Override
    public void setNullable(boolean isNullable) {
        this.isNullable = isNullable;
    }
}

// float64
class Float64Handler implements ColumnTypeHandler {
    private boolean isNullable;

    public Float64Handler() {
        this.isNullable = false;
    }

    public Float64Handler(boolean isNullable) {
        this.isNullable = isNullable;
    }

    @Override
    public Object parseValue(Object value) {
        if (isNullable) {
            return parseNullableValue(value);
        } else {
            return parseNonNullValue(value);
        }
    }

    private Double parseNullableValue(Object value) {
        if (value == null || value.equals("NULL")) {
            return null;
        }
        if (value.equals("NaN") || value.equals("nan")) {
            return Double.NaN;
        }
        if (value.equals("Infinity") || value.equals("inf")) {
            return Double.POSITIVE_INFINITY;
        }
        if (value instanceof String) {
            return Double.parseDouble((String) value);
        }
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        return null;
    }

    private double parseNonNullValue(Object value) {
        if (value == null || value.equals("NULL")) {
            throw new IllegalArgumentException("Float64 type is not nullable");
        }
        if (value.equals("NaN") || value.equals("nan")) {
            return Double.NaN;
        }
        if (value.equals("Infinity") || value.equals("inf")) {
            return Double.POSITIVE_INFINITY;
        }
        if (value instanceof String) {
            return Double.parseDouble((String) value);
        }
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        return 0;
    }

    @Override
    public void setNullable(boolean isNullable) {
        this.isNullable = isNullable;
    }
}

// boolean
class BooleanHandler implements ColumnTypeHandler {
    private boolean isNullable;

    public BooleanHandler() {
        this.isNullable = false;
    }

    public BooleanHandler(boolean isNullable) {
        this.isNullable = isNullable;
    }

    @Override
    public Object parseValue(Object value) {
        if (isNullable) {
            return parseNullableValue(value);
        } else {
            return parseNonNullValue(value);
        }
    }

    /**
     * If boolean column is string, only "1" and "true" will be judged as true,
     * otherwise it is false.
     */
    private static final String TRUE_NUM = "1";
    private static final String TRUE_STRING = "true";

    private Boolean parseNullableValue(Object value) {
        if (value == null || value.equals("NULL")) {
            return null;
        }
        if (value instanceof String) {
            return TRUE_NUM.equals(value) || TRUE_STRING.equals(value);
        }
        if (value instanceof Number) {
            return ((Number) value).intValue() != 0;
        }
        return null;
    }

    private boolean parseNonNullValue(Object value) {
        if (value == null || value.equals("NULL")) {
            throw new IllegalArgumentException("Boolean type is not nullable");
        }
        if (value instanceof String) {
            return TRUE_NUM.equals(value) || TRUE_STRING.equals(value);
        }
        if (value instanceof Number) {
            return ((Number) value).intValue() != 0;
        }
        return false;
    }

    @Override
    public void setNullable(boolean isNullable) {
        this.isNullable = isNullable;
    }
}


class DecimalHandler implements ColumnTypeHandler {
    private boolean isNullable;

    public DecimalHandler() {
        this.isNullable = false;
    }

    public DecimalHandler(boolean isNullable) {
        this.isNullable = isNullable;
    }

    @Override
    public Object parseValue(Object value) {
        if (isNullable) {
            return parseNullableValue(value);
        } else {
            return parseNonNullValue(value);
        }
    }

    private BigDecimal parseNullableValue(Object value) {
        if (value == null || value.equals("NULL")) {
            return null;
        }
        if (value instanceof Integer) {
            return BigDecimal.valueOf((int) value);
        }
        if (value instanceof String) {
            return new BigDecimal((String) value);
        }
        if (value instanceof Number) {
            return new BigDecimal((String) value);
        }
        return null;
    }

    private BigDecimal parseNonNullValue(Object value) {
        if (value == null || value.equals("NULL")) {
            throw new IllegalArgumentException("decimal type is not nullable");
        }
        if (value instanceof Integer) {
            return BigDecimal.valueOf((int) value);
        }
        if (value instanceof String) {
            return new BigDecimal((String) value);
        }
        if (value instanceof Number) {
            return new BigDecimal((String) value);
        }
        return BigDecimal.ZERO;
    }

    @Override
    public void setNullable(boolean isNullable) {
        this.isNullable = isNullable;
    }
}
