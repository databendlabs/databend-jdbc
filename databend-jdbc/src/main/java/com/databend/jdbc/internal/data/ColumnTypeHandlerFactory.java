package com.databend.jdbc.internal.data;

import java.util.Locale;

public class ColumnTypeHandlerFactory {
    public static ColumnTypeHandler getTypeHandler(DatabendRawType type) {
        if (type == null) {
            return null;
        }
        switch (type.getDataType().getDisplayName().toLowerCase(Locale.US)) {
            case DatabendTypes.INT8:
                return new Int8Handler(type.isNullable());
            case DatabendTypes.INT16:
                return new Int16Handler(type.isNullable());
            case DatabendTypes.INT32:
                return new Int32Handler(type.isNullable());
            case DatabendTypes.INT64:
                return new Int64Handler(type.isNullable());
            case DatabendTypes.UINT8:
                return new UInt8Handler(type.isNullable());
            case DatabendTypes.UINT16:
                return new UInt16Handler(type.isNullable());
            case DatabendTypes.UINT32:
                return new UInt32Handler(type.isNullable());
            case DatabendTypes.UINT64:
                return new UInt64Handler(type.isNullable());
            case DatabendTypes.FLOAT32:
                return new Float32Handler(type.isNullable());
            case DatabendTypes.FLOAT64:
                return new Float64Handler(type.isNullable());
            case DatabendTypes.BOOLEAN:
                return new BooleanHandler(type.isNullable());
            case DatabendTypes.DECIMAL:
                return new DecimalHandler(type.isNullable());
            case DatabendTypes.GEOMETRY:
                return new GeometryHandler(type.isNullable());
            case DatabendTypes.ARRAY:
            case DatabendTypes.DATE:
            case DatabendTypes.DATETIME:
            case DatabendTypes.DATETIME64:
            case DatabendTypes.TIMESTAMP:
            case DatabendTypes.STRING:
            case DatabendTypes.NULL:
            case DatabendTypes.STRUCT:
            case DatabendTypes.VARIANT:
            case DatabendTypes.VARIANT_ARRAY:
            case DatabendTypes.VARIANT_OBJECT:
            case DatabendTypes.INTERVAL:
            default:
                return new StringHandler(type.isNullable());
        }
    }
}
