package com.databend.jdbc;

import com.databend.client.data.DatabendDataType;
import com.databend.client.data.DatabendRawType;
import com.databend.client.data.DatabendTypes;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;

import java.sql.Types;
import java.util.List;
import java.util.Locale;

import static java.util.Objects.requireNonNull;

public class DatabendColumnInfo {
    private static final int VARBINARY_MAX = 1024 * 1024 * 1024;
    private static final int TIME_ZONE_MAX = 40; // current longest time zone is 32
    private static final int TIME_MAX = "HH:mm:ss.SSS".length();
    private static final int TIME_WITH_TIME_ZONE_MAX = TIME_MAX + TIME_ZONE_MAX;
    private static final int TIMESTAMP_MAX = "yyyy-MM-dd HH:mm:ss.SSS".length();
    private static final int TIMESTAMP_WITH_TIME_ZONE_MAX = TIMESTAMP_MAX + TIME_ZONE_MAX;
    private static final int DATE_MAX = "yyyy-MM-dd".length();

    private final int columnType;
    private final String columnName;
    private final List<Integer> columnParameterTypes;
    private final DatabendRawType type;
    private final Nullable nullable;
    private final boolean currency;
    private final boolean signed;
    private final int precision;
    private final int scale;
    private final int columnDisplaySize;
    private final String columnLabel;

    private final String tableName;
    private final String schemaName;
    private final String catalogName;

    public DatabendColumnInfo(int columnType, List<Integer> columnParameterTypes, DatabendRawType type, Nullable nullable, boolean currency, boolean signed, int precision, int scale, int columnDisplaySize, String columnLabel, String columnName, String tableName, String schemaName, String catalogName) {
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

    public static DatabendColumnInfo of(String name, DatabendRawType type) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(name), "Provided name is null or empty");
        return newBuilder(name, type).build();
    }

    public static void setTypeInfo(Builder builder, DatabendRawType type) {
        builder.setColumnType(type.getDataType().getSqlType());
        boolean isNullable = type.isNullable();
        builder.setNullable(isNullable ? Nullable.NULLABLE : Nullable.NO_NULLS);
        switch (type.getDataType().getDisplayName()) {
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
            case DatabendTypes.DECIMAL:
                builder.setSigned(true);
                builder.setScale(type.getDecimalDigits());
                builder.setPrecision(type.getColumnSize());
                builder.setColumnDisplaySize(type.getColumnSize());
                break;
        }

    }

    public static Builder newBuilder(String name, DatabendRawType type) {
        return (new Builder()).setColumnName(name).setColumnType(type.getDataType().getSqlType());
    }

    public int getColumnType() {
        return columnType;
    }

    public List<Integer> getColumnParameterTypes() {
        return columnParameterTypes;
    }

    public DatabendRawType getType() {
        return type;
    }

    public Nullable getNullable() {
        return nullable;
    }

    public String getColumnTypeName() {
        return type.getDataType().getDisplayName();
    }

    public boolean isCurrency() {
        return currency;
    }

    public boolean isSigned() {
        return signed;
    }

    public int getPrecision() {
        return precision;
    }

    public int getScale() {
        return scale;
    }

    public int getColumnDisplaySize() {
        return columnDisplaySize;
    }

    public String getColumnLabel() {
        return columnLabel;
    }

    public String getColumnName() {
        return columnName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getCatalogName() {
        return catalogName;
    }


    public enum Nullable {
        NO_NULLS, NULLABLE, UNKNOWN
    }

    // builder
    public static final class Builder {
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

        Builder() {
        }

        private Builder(DatabendColumnInfo databendColumnInfo) {
            this.columnName = databendColumnInfo.columnName;
            this.columnType = databendColumnInfo.columnType;
            this.columnLabel = databendColumnInfo.columnLabel;
            this.type = databendColumnInfo.type;
            this.columnDisplaySize = databendColumnInfo.columnDisplaySize;
            this.tableName = databendColumnInfo.tableName;
            this.schemaName = databendColumnInfo.schemaName;
            this.catalogName = databendColumnInfo.catalogName;
            this.scale = databendColumnInfo.scale;
            this.columnParameterTypes = databendColumnInfo.columnParameterTypes;
            this.signed = databendColumnInfo.signed;
            this.currency = databendColumnInfo.currency;
            this.nullable = databendColumnInfo.nullable;
        }


        public Builder setColumnType(int columnType) {
            this.columnType = columnType;
            return this;
        }

        public void setColumnParameterTypes(List<Integer> columnParameterTypes) {
            this.columnParameterTypes = ImmutableList.copyOf(requireNonNull(columnParameterTypes, "columnParameterTypes is null"));
        }

        public Builder setColumnTypeSignature(DatabendRawType columnTypeSignature) {
            this.type = columnTypeSignature;
            return this;
        }

        public Builder setNullable(Nullable nullable) {
            this.nullable = nullable;
            return this;
        }

        public Builder setCurrency(boolean currency) {
            this.currency = currency;
            return this;
        }

        public Builder setSigned(boolean signed) {
            this.signed = signed;
            return this;
        }

        public Builder setPrecision(int precision) {
            this.precision = precision;
            return this;
        }

        public Builder setScale(int scale) {
            this.scale = scale;
            return this;
        }

        public Builder setColumnDisplaySize(int columnDisplaySize) {
            this.columnDisplaySize = columnDisplaySize;
            return this;
        }

        public Builder setColumnLabel(String columnLabel) {
            this.columnLabel = columnLabel;
            return this;
        }

        public Builder setColumnName(String columnName) {
            this.columnName = columnName;
            return this;
        }

        public Builder setTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder setSchemaName(String schemaName) {
            this.schemaName = schemaName;
            return this;
        }

        public Builder setCatalogName(String catalogName) {
            this.catalogName = catalogName;
            return this;
        }

        public DatabendColumnInfo build() {
            return new DatabendColumnInfo(
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
