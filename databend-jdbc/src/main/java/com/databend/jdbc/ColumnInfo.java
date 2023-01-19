package com.databend.jdbc;

import com.databend.client.data.DatabendRawType;
import com.databend.client.data.DatabendTypes;
import com.google.common.collect.ImmutableList;

import java.sql.Types;
import java.util.List;

import static java.util.Objects.requireNonNull;

class ColumnInfo
{
    private static final int VARBINARY_MAX = 1024 * 1024 * 1024;
    private static final int TIME_ZONE_MAX = 40; // current longest time zone is 32
    private static final int TIME_MAX = "HH:mm:ss.SSS".length();
    private static final int TIME_WITH_TIME_ZONE_MAX = TIME_MAX + TIME_ZONE_MAX;
    private static final int TIMESTAMP_MAX = "yyyy-MM-dd HH:mm:ss.SSS".length();
    private static final int TIMESTAMP_WITH_TIME_ZONE_MAX = TIMESTAMP_MAX + TIME_ZONE_MAX;
    private static final int DATE_MAX = "yyyy-MM-dd".length();

    private final int columnType;
    private final List<Integer> columnParameterTypes;
    private final DatabendRawType type;
    private final Nullable nullable;
    private final boolean currency;
    private final boolean signed;
    private final int precision;
    private final int scale;
    private final int columnDisplaySize;
    private final String columnLabel;
    private final String columnName;
    private final String tableName;
    private final String schemaName;
    private final String catalogName;

    public ColumnInfo(int columnType, List<Integer> columnParameterTypes, DatabendRawType type, Nullable nullable, boolean currency, boolean signed, int precision, int scale, int columnDisplaySize, String columnLabel, String columnName, String tableName, String schemaName, String catalogName)
    {
        this.columnType = columnType;
        this.columnParameterTypes = columnParameterTypes;
        this.type = type;
        this.nullable = nullable;
        this.currency = currency;
        this.signed = signed;
        this.precision = precision;
        this.scale = scale;
        this.columnDisplaySize = columnDisplaySize;
        this.columnLabel = columnLabel;
        this.columnName = columnName;
        this.tableName = tableName;
        this.schemaName = schemaName;
        this.catalogName = catalogName;
    }

    public static void setTypeInfo(Builder builder, DatabendRawType type)
    {
        builder.setColumnType(getType(type));
        boolean isNullable = type.isNullable();
        builder.setNullable(isNullable ? Nullable.NULLABLE : Nullable.NO_NULLS);
        switch (type.getType()) {
            case DatabendTypes.BOOLEAN:
                builder.setColumnDisplaySize(5);
                break;
            case DatabendTypes.UINT8:
                builder.setSigned(false);
                builder.setPrecision(3);
                builder.setColumnDisplaySize(4);
                builder.setScale(0);
                break;
            case DatabendTypes.INT8:
                builder.setSigned(true);
                builder.setPrecision(4);
                builder.setColumnDisplaySize(5);
                builder.setScale(0);
                break;
            case DatabendTypes.UINT16:
                builder.setSigned(false);
                builder.setPrecision(5);
                builder.setColumnDisplaySize(6);
                builder.setScale(0);
                break;
            case DatabendTypes.INT16:
                builder.setSigned(true);
                builder.setPrecision(5);
                builder.setColumnDisplaySize(6);
                builder.setScale(0);
                break;
            case DatabendTypes.UINT32:
                builder.setSigned(false);
                builder.setPrecision(10);
                builder.setColumnDisplaySize(11);
                builder.setScale(0);
                break;
            case DatabendTypes.INT32:
                builder.setSigned(true);
                builder.setPrecision(10);
                builder.setColumnDisplaySize(11);
                builder.setScale(0);
                break;
            case DatabendTypes.UINT64:
                builder.setSigned(false);
                builder.setPrecision(19);
                builder.setColumnDisplaySize(20);
                builder.setScale(0);
                break;
            case DatabendTypes.INT64:
                builder.setSigned(true);
                builder.setPrecision(19);
                builder.setColumnDisplaySize(20);
                builder.setScale(0);
                break;
            case DatabendTypes.FLOAT32:
                builder.setSigned(true);
                builder.setPrecision(9);
                builder.setColumnDisplaySize(16);
                builder.setScale(0);
                break;
            case DatabendTypes.FLOAT64:
                builder.setSigned(true);
                builder.setPrecision(17);
                builder.setColumnDisplaySize(24);
                builder.setScale(0);
                break;
            case DatabendTypes.STRING:
                builder.setSigned(false);
                builder.setScale(0);
                builder.setPrecision(VARBINARY_MAX);
                builder.setColumnDisplaySize(VARBINARY_MAX);
                break;
            case DatabendTypes.DATE:
                builder.setSigned(false);
                builder.setScale(0);
                builder.setPrecision(DATE_MAX);
                builder.setColumnDisplaySize(DATE_MAX);
                break;
            case DatabendTypes.DATETIME:
                builder.setSigned(false);
                builder.setScale(0);
                builder.setPrecision(TIMESTAMP_MAX);
                builder.setColumnDisplaySize(TIMESTAMP_MAX);
                break;
            case DatabendTypes.DATETIME64:
                builder.setSigned(false);
                builder.setScale(0);
                builder.setPrecision(TIMESTAMP_MAX);
                builder.setColumnDisplaySize(TIMESTAMP_MAX);
                break;

        }

    }

    private static int getType(DatabendRawType type) {
        if (type ==null) {
            return java.sql.Types.NULL;
        }
//        if (type.isNullable()) {
//            return getType(type.getInner());
//        }
        switch (type.getType()) {
            case DatabendTypes.BOOLEAN:
                return java.sql.Types.BOOLEAN;
            case DatabendTypes.UINT8:
                return java.sql.Types.TINYINT;
            case DatabendTypes.INT8:
                return java.sql.Types.TINYINT;
            case DatabendTypes.UINT16:
                return java.sql.Types.SMALLINT;
            case DatabendTypes.INT16:
                return java.sql.Types.SMALLINT;
            case DatabendTypes.UINT32:
                return java.sql.Types.INTEGER;
            case DatabendTypes.INT32:
                return java.sql.Types.INTEGER;
            case DatabendTypes.UINT64:
                return java.sql.Types.BIGINT;
            case DatabendTypes.INT64:
                return java.sql.Types.BIGINT;
            case DatabendTypes.FLOAT32:
                return java.sql.Types.REAL;
            case DatabendTypes.FLOAT64:
                return java.sql.Types.DOUBLE;
            case DatabendTypes.STRING:
                return java.sql.Types.VARCHAR;
            case DatabendTypes.DATE:
                return java.sql.Types.DATE;
            case DatabendTypes.DATETIME:
                return java.sql.Types.TIMESTAMP;
            case DatabendTypes.DATETIME64:
                return java.sql.Types.TIMESTAMP;
            case DatabendTypes.TIMESTAMP:
                return java.sql.Types.TIMESTAMP;
            case DatabendTypes.ARRAY:
                return java.sql.Types.ARRAY;
            default:
                return Types.JAVA_OBJECT;
        }
    }

    public int getColumnType()
    {
        return columnType;
    }

    public List<Integer> getColumnParameterTypes()
    {
        return columnParameterTypes;
    }

    public DatabendRawType getType()
    {
        return type;
    }

    public Nullable getNullable()
    {
        return nullable;
    }

    public String getColumnTypeName()
    {
        return type.toString();
    }

    public boolean isCurrency()
    {
        return currency;
    }

    public boolean isSigned()
    {
        return signed;
    }

    public int getPrecision()
    {
        return precision;
    }

    public int getScale()
    {
        return scale;
    }

    public int getColumnDisplaySize()
    {
        return columnDisplaySize;
    }

    public String getColumnLabel()
    {
        return columnLabel;
    }

    public String getColumnName()
    {
        return columnName;
    }

    public String getTableName()
    {
        return tableName;
    }

    public String getSchemaName()
    {
        return schemaName;
    }

    public String getCatalogName()
    {
        return catalogName;
    }


    public enum Nullable
    {
        NO_NULLS, NULLABLE, UNKNOWN
    }

    // builder
    static class Builder
    {
        private int columnType;
        private List<Integer> columnParameterTypes;
        private DatabendRawType type;
        private Nullable nullable;
        private boolean currency;
        private boolean signed;
        private int precision;
        private int scale;
        private int columnDisplaySize;
        private String columnLabel;
        private String columnName;
        private String tableName;
        private String schemaName;
        private String catalogName;

        public Builder setColumnType(int columnType)
        {
            this.columnType = columnType;
            return this;
        }

        public void setColumnParameterTypes(List<Integer> columnParameterTypes)
        {
            this.columnParameterTypes = ImmutableList.copyOf(requireNonNull(columnParameterTypes, "columnParameterTypes is null"));
        }

        public Builder setColumnTypeSignature(DatabendRawType columnTypeSignature)
        {
            this.type = columnTypeSignature;
            return this;
        }

        public Builder setNullable(Nullable nullable)
        {
            this.nullable = nullable;
            return this;
        }

        public Builder setCurrency(boolean currency)
        {
            this.currency = currency;
            return this;
        }

        public Builder setSigned(boolean signed)
        {
            this.signed = signed;
            return this;
        }

        public Builder setPrecision(int precision)
        {
            this.precision = precision;
            return this;
        }

        public Builder setScale(int scale)
        {
            this.scale = scale;
            return this;
        }

        public Builder setColumnDisplaySize(int columnDisplaySize)
        {
            this.columnDisplaySize = columnDisplaySize;
            return this;
        }

        public Builder setColumnLabel(String columnLabel)
        {
            this.columnLabel = columnLabel;
            return this;
        }

        public Builder setColumnName(String columnName)
        {
            this.columnName = columnName;
            return this;
        }

        public Builder setTableName(String tableName)
        {
            this.tableName = tableName;
            return this;
        }

        public Builder setSchemaName(String schemaName)
        {
            this.schemaName = schemaName;
            return this;
        }

        public Builder setCatalogName(String catalogName)
        {
            this.catalogName = catalogName;
            return this;
        }

        public ColumnInfo build()
        {
            return new ColumnInfo(
                    columnType,
                    columnParameterTypes,
                    type,
                    nullable,
                    currency,
                    signed,
                    precision,
                    scale,
                    columnDisplaySize,
                    columnLabel,
                    columnName,
                    tableName,
                    schemaName,
                    catalogName);
        }
    }


}
