package com.databend.jdbc;

import com.databend.client.data.DatabendDataType;

import java.sql.Types;

public class JdbcTypeMapping {
    /**
     * Converts {@link DatabendColumnInfo} to generic SQL type defined in JDBC.
     *
     * @param column non-null column definition
     * @return generic SQL type defined in JDBC
     */
    public int toSqlType(DatabendColumnInfo column) {
        DatabendDataType dataType = column.getType().getDataType();
        int sqlType = Types.OTHER;
        switch (dataType) {
            case BOOLEAN:
                sqlType = Types.BOOLEAN;
                break;
            case INT_8:
                sqlType = Types.TINYINT;
                break;
            case INT_16:
                sqlType = Types.SMALLINT;
                break;
            case INT_32:
                sqlType = Types.INTEGER;
                break;
            case INT_64:
                sqlType = Types.BIGINT;
                break;
            case FLOAT:
                sqlType = Types.FLOAT;
                break;
            case DOUBLE:
                sqlType = Types.DOUBLE;
                break;
            case DECIMAL:
                sqlType = Types.DECIMAL;
                break;
            case STRING:
                sqlType = Types.VARCHAR;
                break;
            case DATE:
                sqlType = Types.DATE;
                break;
            case TIMESTAMP:
                sqlType = Types.TIMESTAMP;
                break;
            case ARRAY:
                sqlType = Types.ARRAY;
                break;
            case VARIANT:
                sqlType = Types.VARCHAR;
                break;
            case TUPLE:
                sqlType = Types.STRUCT;
                break;
            case NULL:
                sqlType = Types.NULL;
                break;
            default:
                break;
        }
        return sqlType;
    }

    /**
     * Gets corresponding {@link DatabendDataType} of the given {@link Types}.
     *
     * @param sqlType generic SQL types defined in JDBC
     * @return non-null Databend data type
     */
    protected DatabendDataType getDataType(int sqlType) {
        DatabendDataType dataType;

        switch (sqlType) {
            case Types.BOOLEAN:
                dataType = DatabendDataType.UNSIGNED_INT_8;
                break;
            case Types.TINYINT:
                dataType = DatabendDataType.INT_8;
                break;
            case Types.SMALLINT:
                dataType = DatabendDataType.INT_16;
                break;
            case Types.INTEGER:
                dataType = DatabendDataType.INT_32;
                break;
            case Types.BIGINT:
                dataType = DatabendDataType.INT_64;
                break;
            case Types.FLOAT:
                dataType = DatabendDataType.FLOAT;
                break;
            case Types.DOUBLE:
                dataType = DatabendDataType.DOUBLE;
                break;
            case Types.DECIMAL:
                dataType = DatabendDataType.DECIMAL;
                break;
            case Types.BIT:
            case Types.BLOB:
            case Types.BINARY:
            case Types.CHAR:
            case Types.CLOB:
            case Types.JAVA_OBJECT:
            case Types.LONGNVARCHAR:
            case Types.LONGVARBINARY:
            case Types.LONGVARCHAR:
            case Types.NCHAR:
            case Types.NCLOB:
            case Types.NVARCHAR:
            case Types.OTHER:
            case Types.SQLXML:
            case Types.VARBINARY:
            case Types.VARCHAR:
                dataType = DatabendDataType.STRING;
                break;
            case Types.DATE:
                dataType = DatabendDataType.DATE;
                break;
            case Types.TIME:
            case Types.TIME_WITH_TIMEZONE:
            case Types.TIMESTAMP:
            case Types.TIMESTAMP_WITH_TIMEZONE:
                dataType = DatabendDataType.TIMESTAMP;
                break;
            case Types.ARRAY:
                dataType = DatabendDataType.ARRAY;
                break;
            case Types.STRUCT:
                dataType = DatabendDataType.TUPLE;
                break;
            case Types.DATALINK:
            case Types.DISTINCT:
            case Types.REF:
            case Types.REF_CURSOR:
            case Types.ROWID:
            case Types.NULL:
            default:
                dataType = DatabendDataType.NULL;
                break;
        }
        return dataType;
    }
}
