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

class Int8Handler extends ColumnTypeHandlerBase  {

    public Int8Handler(boolean isNullable) {
      super(isNullable);
    }

    @Override
    public Object parseString(String value) {
        return Byte.parseByte(value);
    }
}

class Int16Handler extends ColumnTypeHandlerBase  {
    public Int16Handler(boolean isNullable) {
      super(isNullable);
    }

    @Override
    public Object parseString(String value) {
        return Short.parseShort(value);
    }
}

// int32
class Int32Handler extends ColumnTypeHandlerBase  {
    public Int32Handler(boolean isNullable) {
      super(isNullable);
    }

    @Override
    public Object parseString(String value) {
        return Integer.parseInt(value);
    }
}

// int64
class Int64Handler extends ColumnTypeHandlerBase  {
    public Int64Handler(boolean isNullable) {
      super(isNullable);
    }

    @Override
    public Object parseString(String  value) {
        return Long.parseLong(value);
    }
}

// uint8
class UInt8Handler extends ColumnTypeHandlerBase {
    public UInt8Handler(boolean isNullable) {
      super(isNullable);
    }

    @Override
    public Object parseString(String  value) {
        return Short.parseShort(value);
    }
}

// uint16
class UInt16Handler extends ColumnTypeHandlerBase  {

    public UInt16Handler(boolean isNullable) {
      super(isNullable);
    }

    @Override
    public Object parseString(String  value) {
        return Integer.parseInt(value);
    }
}

// uint32
class UInt32Handler extends ColumnTypeHandlerBase  {
    public UInt32Handler(boolean isNullable) {
      super(isNullable);
    }

    @Override
    public Object parseString(String  value) {
        return Long.parseLong(value);
    }
}

// uint64
class UInt64Handler extends ColumnTypeHandlerBase  {
    public UInt64Handler(boolean isNullable) {
      super(isNullable);
    }

    @Override
    public Object parseString(String  value) {
        return new BigInteger(value);
    }
}

// float32
class Float32Handler extends ColumnTypeHandlerBase  {

    public Float32Handler(boolean isNullable) {
      super(isNullable);
    }

    @Override
    public Object parseString(String  value) {
        if (value.equals("NaN") || value.equals("nan")) {
            return Double.NaN;
        }
        if (value.equals("Infinity") || value.equals("inf")) {
            return Double.POSITIVE_INFINITY;
        }
        return Float.parseFloat(value);
    }
}

// float64
class Float64Handler extends ColumnTypeHandlerBase  {
    public Float64Handler(boolean isNullable) {
      super(isNullable);
    }

    @Override
    public Object parseString(String  value) {
        if (value.equals("NaN") || value.equals("nan")) {
            return Double.NaN;
        }
        if (value.equals("Infinity") || value.equals("inf")) {
            return Double.POSITIVE_INFINITY;
        }
        return Double.parseDouble(value);
    }
}

// boolean
/**
 * If boolean column is string, only "1" and "true" will be judged as true,
 * otherwise it is false.
 */
class BooleanHandler extends ColumnTypeHandlerBase {
    private static final String TRUE_NUM = "1";
    private static final String TRUE_STRING = "true";

    public BooleanHandler(boolean isNullable) {
     super(isNullable);;
    }

    @Override
    public Object parseString(String value) {
        return TRUE_NUM.equals(value) || TRUE_STRING.equals(value);
    }
}


class DecimalHandler extends ColumnTypeHandlerBase {
    public DecimalHandler(boolean isNullable) {
     super(isNullable);;
    }

    @Override
    public Object parseString(String value) {
        return new BigDecimal(value);
    }
}
