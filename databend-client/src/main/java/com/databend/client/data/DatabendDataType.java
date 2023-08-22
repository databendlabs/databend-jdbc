package com.databend.client.data;

import java.sql.Types;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

// data type that reflect java.sql.type
public enum DatabendDataType {
    BOOLEAN(Types.BOOLEAN, DatabendTypes.BOOLEAN, true, false, 1, 0, 0, false, "Boolean", "BOOL"),
    INTEGER(Types.INTEGER, DatabendTypes.INTEGER, true, false, 0, 0, 0, false, "Int32", "INTEGER",
            "INT", "Int8", "Int16", "UInt16", "UInt32"),
    INT_8(Types.INTEGER, DatabendTypes.INT8, true, false, 0, 0, 0, false,
            "INT", "Int8"),
    U_INT_8(Types.TINYINT, DatabendTypes.UINT8, true, false, 3, 0, 0, false, "UInt8"),
    INT_16(Types.INTEGER, DatabendTypes.INT16, true, false, 0, 0, 0, false, "Int16"),
    UNSIGNED_BIG_INT_16(Types.BIGINT, DatabendTypes.UINT16, false, false, 0, 0, 0, false, "UInt16"),
    INT_32(Types.INTEGER, DatabendTypes.INT32, true, false, 0, 0, 0, false, "Int32"),
    UNSIGNED_BIG_INT_32(Types.BIGINT, DatabendTypes.UINT32, false, false, 0, 0, 0, false, "UInt32"),
    INT_64(Types.INTEGER, DatabendTypes.INT64, true, false, 0, 0, 0, false, "Int64"),
    UNSIGNED_BIG_INT_64(Types.BIGINT, DatabendTypes.UINT64, false, false, 0, 0, 0, false, "UInt64"),
    DOUBLE(Types.DOUBLE, DatabendTypes.FLOAT64, true, false, 0, 0, 0, false,
            "Float64", "DOUBLE"),
    FLOAT(Types.FLOAT, DatabendTypes.FLOAT32, true, false, 0, 0, 0, false,
            "Float32", "Float"),

    STRING(Types.VARCHAR, DatabendTypes.STRING, false, true, 0, 0, 0, false, "String", "VARCHAR"),

    DATE(Types.DATE, DatabendTypes.DATE, false, false, 0, 0, 0, true, "Date"),
    TIMESTAMP(Types.TIMESTAMP, DatabendTypes.TIMESTAMP, false, false, 6, 0, 0, true, "DateTime", "TIMESTAMP"),

    NULL(Types.NULL, DatabendTypes.NULL, false, false, 0, 0, 0, false, "NULL"),
    ARRAY(Types.ARRAY, DatabendTypes.ARRAY, false, true, 0, 0, 0, false, "Array"),
    MAP(Types.OTHER, DatabendTypes.MAP, false, true, 0, 0, 0, false, "Map"),
    TUPLE(Types.OTHER, DatabendTypes.TUPLE, false, true, 0, 0, 0, false, "Tuple"),
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
    private final boolean caseSensitive;
    private final int precision;

    private final int minScale;
    private final int maxScale;

    private final boolean time;
    private final String[] aliases;

    DatabendDataType(int sqlType, String displayName, boolean signed,
                     boolean caseSensitive, int precision, int minScale, int maxScale, boolean isTime, String... aliases) {
        this.sqlType = sqlType;
        this.displayName = displayName;
        this.signed = signed;
        this.caseSensitive = caseSensitive;
        this.precision = precision;
        this.maxScale = maxScale;
        this.aliases = aliases;
        this.time = isTime;
        this.minScale = minScale;
    }

    public static DatabendDataType ofType(String type) {
        String formattedType = type.trim().toUpperCase();
        return Optional.ofNullable(typeNameOrAliasToType.get(formattedType)).orElse(NULL);
    }

}
