package com.databend.jdbc.metadata;

import lombok.experimental.UtilityClass;

/**
 * Column names to include in the {@link java.sql.ResultSet} as specified in the
 * {@link java.sql.DatabaseMetaData} javadoc
 */
@UtilityClass
public class MetadataColumns {
    public static final String TABLE_CAT = "TABLE_CAT";
    public static final String TABLE_CATALOG = "TABLE_CATALOG";
    public static final String TABLE_SCHEM = "TABLE_SCHEM";
    public static final String TABLE_NAME = "TABLE_NAME";
    public static final String COLUMN_NAME = "COLUMN_NAME";
    public static final String DATA_TYPE = "DATA_TYPE";
    public static final String TYPE_NAME = "TYPE_NAME";
    public static final String COLUMN_SIZE = "COLUMN_SIZE";
    public static final String BUFFER_LENGTH = "BUFFER_LENGTH";
    public static final String DECIMAL_DIGITS = "DECIMAL_DIGITS";
    public static final String NULLABLE = "NULLABLE";
    public static final String REMARKS = "REMARKS";
    public static final String COLUMN_DEF = "COLUMN_DEF";
    public static final String CHAR_OCTET_LENGTH = "CHAR_OCTET_LENGTH";
    public static final String ORDINAL_POSITION = "ORDINAL_POSITION";
    public static final String IS_NULLABLE = "IS_NULLABLE";
    public static final String SCOPE_CATALOG = "SCOPE_CATALOG";
    public static final String SCOPE_SCHEMA = "SCOPE_SCHEMA";
    public static final String SCOPE_TABLE = "SCOPE_TABLE";
    public static final String SOURCE_DATA_TYPE = "SOURCE_DATA_TYPE";
    public static final String IS_AUTOINCREMENT = "IS_AUTOINCREMENT";
    public static final String IS_GENERATEDCOLUMN = "IS_GENERATEDCOLUMN";
    public static final String TABLE_TYPE = "TABLE_TYPE";
    public static final String TYPE_CAT = "TYPE_CAT";
    public static final String TYPE_SCHEM = "TYPE_SCHEM";
    public static final String SELF_REFERENCING_COL_NAME = "SELF_REFERENCING_COL_NAME";
    public static final String REF_GENERATION = "REF_GENERATION";
    public static final int COMMON_RADIX = 10;
    public static final String PRECISION = "PRECISION";
    public static final String LITERAL_PREFIX = "LITERAL_PREFIX";
    public static final String LITERAL_SUFFIX = "LITERAL_SUFFIX";
    public static final String CREATE_PARAMS = "CREATE_PARAMS";
    public static final String CASE_SENSITIVE = "CASE_SENSITIVE";
    public static final String SEARCHABLE = "SEARCHABLE";
    public static final String UNSIGNED_ATTRIBUTE = "UNSIGNED_ATTRIBUTE";
    public static final String FIXED_PREC_SCALE = "FIXED_PREC_SCALE";
    public static final String AUTO_INCREMENT = "AUTO_INCREMENT";
    public static final String LOCAL_TYPE_NAME = "LOCAL_TYPE_NAME";
    public static final String MINIMUM_SCALE = "MINIMUM_SCALE";
    public static final String MAXIMUM_SCALE = "MAXIMUM_SCALE";
    public static final String SQL_DATA_TYPE = "SQL_DATA_TYPE";
    public static final String SQL_DATETIME_SUB = "SQL_DATETIME_SUB";
    public static final String NUM_PREC_RADIX = "NUM_PREC_RADIX";

}

