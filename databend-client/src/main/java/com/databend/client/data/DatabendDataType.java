package com.databend.client.data;


import java.sql.Types;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.databend.client.data.DatabendRawType.startsWithIgnoreCase;
import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * data type that reflect java.sql.type
 */
public enum DatabendDataType {

    BOOLEAN(Types.BOOLEAN, DatabendTypes.BOOLEAN, false, 1, false, "Boolean", "BOOL"),

    // int8 -> TINYINT -> -128~127
    INT_8(Types.TINYINT, DatabendTypes.INT8, true, 3, false, "Int8", "TINYINT"),
    UNSIGNED_INT_8(Types.TINYINT, DatabendTypes.UINT8, false, 3, false, "UInt8", "TINYINT UNSIGNED"),

    // int16 -> SMALLINT -> -32768~32767
    INT_16(Types.SMALLINT, DatabendTypes.INT16, true, 5, false, "Int16", "SMALLINT"),
    UNSIGNED_INT_16(Types.SMALLINT, DatabendTypes.UINT16, false, 5, false, "UInt16", "SMALLINT UNSIGNED"),

    // int32 -> INT -> -2147483648~2147483647
    INT_32(Types.INTEGER, DatabendTypes.INT32, true, 10, false, "Int32", "INTEGER"),
    UNSIGNED_INT_32(Types.INTEGER, DatabendTypes.UINT32, false, 10, false, "UInt32", "INTEGER UNSIGNED"),

    // INT64 -> BIGINT -> -9223372036854775808~9223372036854775807
    INT_64(Types.BIGINT, DatabendTypes.INT64, true, 19, false, "Int64", "BIGINT"),
    UNSIGNED_INT_64(Types.BIGINT, DatabendTypes.UINT64, false, 20, false, "UInt64", "BIGINT UNSIGNED"),

    DOUBLE(Types.DOUBLE, DatabendTypes.FLOAT64, true, 22, false, "Float64", "DOUBLE"),
    FLOAT(Types.FLOAT, DatabendTypes.FLOAT32, true, 12, false, "Float32", "FLOAT"),
    DECIMAL(Types.DECIMAL, DatabendTypes.DECIMAL, true, 65, false, "Decimal"),

    STRING(Types.VARCHAR, DatabendTypes.STRING, false, Integer.MAX_VALUE, false, "String", "VARCHAR"),

    DATE(Types.DATE, DatabendTypes.DATE, false, 10, true, "Date"),
    TIMESTAMP(Types.TIMESTAMP, DatabendTypes.TIMESTAMP, false, 26, true, "DateTime", "TIMESTAMP"),

    ARRAY(Types.ARRAY, DatabendTypes.ARRAY, false, 0, false, "Array"),
    MAP(Types.OTHER, DatabendTypes.MAP, false, 0, false, "Map"),
    BITMAP(Types.OTHER, DatabendTypes.MAP, false, 0, false, "Bitmap"),
    TUPLE(Types.OTHER, DatabendTypes.TUPLE, false, 0, false, "Tuple"),
    VARIANT(Types.VARCHAR, DatabendTypes.VARIANT, false, 0, false, "Variant", "Json"),

    NULL(Types.NULL, DatabendTypes.NULL, false, 0, false, "NULL"),
    ;

    private static final Map<String, DatabendDataType> typeNameOrAliasToType;

    static {
        typeNameOrAliasToType = new HashMap<>();
        for (DatabendDataType dataType : values()) {
            Arrays.stream(dataType.aliases).forEach(alias -> typeNameOrAliasToType.put(alias.toUpperCase(), dataType));
        }
    }

    private final int sqlType;
    private final String displayName;
    private final boolean signed;
    private final int length;
    private final boolean time;
    private final String[] aliases;

    /**
     * Get Databend data type by full type name
     *
     * @param typeName full Databend data type name
     * @return {@link DatabendDataType}
     */
    public static DatabendDataType getByTypeName(String typeName) {
        // the order of checks is important because some short names could match parts of longer names
        if (DatabendTypes.BOOLEAN.equalsIgnoreCase(typeName)) {
            return BOOLEAN;
        } else if (DatabendTypes.INT8.equalsIgnoreCase(typeName)) {
            return INT_8;
        } else if (DatabendTypes.UINT8.equalsIgnoreCase(typeName)) {
            return UNSIGNED_INT_8;
        } else if (DatabendTypes.INT16.equalsIgnoreCase(typeName)) {
            return INT_16;
        } else if (DatabendTypes.UINT16.equalsIgnoreCase(typeName)) {
            return UNSIGNED_INT_16;
        } else if (DatabendTypes.INT32.equalsIgnoreCase(typeName) || "int".equalsIgnoreCase(typeName)) {
            return INT_32;
        } else if (DatabendTypes.UINT32.equalsIgnoreCase(typeName)) {
            return UNSIGNED_INT_32;
        } else if (DatabendTypes.INT64.equalsIgnoreCase(typeName)) {
            return INT_64;
        } else if (DatabendTypes.UINT64.equalsIgnoreCase(typeName)) {
            return UNSIGNED_INT_64;
        } else if (DatabendTypes.FLOAT32.equalsIgnoreCase(typeName)) {
            return FLOAT;
        } else if (DatabendTypes.FLOAT64.equalsIgnoreCase(typeName)) {
            return DOUBLE;
        } else if (DatabendTypes.DATE.equalsIgnoreCase(typeName)) {
            return DATE;
        } else if (DatabendTypes.TIMESTAMP.equalsIgnoreCase(typeName)) {
            return TIMESTAMP;
        } else if (DatabendTypes.VARIANT.equalsIgnoreCase(typeName)) {
            return VARIANT;
        } else if (DatabendTypes.BITMAP.equalsIgnoreCase(typeName)) {
            return BITMAP;
        } else if (startsWithIgnoreCase(typeName, DatabendTypes.DECIMAL)) {
            return DECIMAL;
        } else if (startsWithIgnoreCase(typeName, DatabendTypes.STRING)) {
            return STRING;
        } else if (startsWithIgnoreCase(typeName, DatabendTypes.ARRAY)) {
            return ARRAY;
        } else if (startsWithIgnoreCase(typeName, DatabendTypes.MAP)) {
            return MAP;
        } else if (startsWithIgnoreCase(typeName, DatabendTypes.TUPLE)) {
            return TUPLE;
        }
        return NULL;
    }

    DatabendDataType(int sqlType, String displayName, boolean signed, int length, boolean isTime, String... aliases) {
        this.sqlType = sqlType;
        this.displayName = displayName;
        this.signed = signed;
        this.length = length;
        this.aliases = aliases;
        this.time = isTime;
    }

    public int getSqlType() {
        return sqlType;
    }

    public String getDisplayName() {
        return displayName;
    }

    public boolean isSigned() {
        return signed;
    }

    public int getLength() {
        return length;
    }

    public boolean isTime() {
        return time;
    }

    public String[] getAliases() {
        return aliases;
    }

    public static DatabendDataType ofType(String type) {
        String formattedType = type.trim().toUpperCase();
        return Optional.ofNullable(typeNameOrAliasToType.get(formattedType)).orElse(NULL);
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("sqlType", sqlType)
                .add("displayName", displayName)
                .add("signed", signed)
                .add("length", length)
                .add("time", time)
                .add("aliases", Arrays.asList(aliases))
                .toString();
    }
}
