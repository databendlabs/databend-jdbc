package com.databend.jdbc;

import com.google.common.base.Joiner;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.RowIdLifetime;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.databend.jdbc.DriverInfo.DRIVER_NAME;
import static com.databend.jdbc.DriverInfo.DRIVER_VERSION;
import static com.databend.jdbc.DriverInfo.DRIVER_VERSION_MAJOR;
import static com.databend.jdbc.DriverInfo.DRIVER_VERSION_MINOR;
import static com.databend.jdbc.metadata.MetadataColumns.*;
import static java.util.Objects.requireNonNull;

public class DatabendDatabaseMetaData implements DatabaseMetaData
{
    private static final String SEARCH_STRING_ESCAPE = "\\";
    private final DatabendConnection connection;

    public DatabendDatabaseMetaData(DatabendConnection connection)
            throws SQLException
    {
        requireNonNull(connection, "connection is null");
        this.connection = connection;
    }

    private static void buildFilters(StringBuilder out, List<String> filters)
    {
        if (!filters.isEmpty()) {
            out.append("\nWHERE ");
            Joiner.on(" AND ").appendTo(out, filters);
        }
    }

    private static void optionalStringLikeFilter(List<String> filters, String columnName, String value)
    {
        if (value != null) {
            filters.add(stringColumnLike(columnName, value));
        }
    }

    private static void optionalStringInFilter(List<String> filters, String columnName, String[] values)
    {
        if (values == null || values.length == 0) {
            return;
        }



        StringBuilder filter = new StringBuilder();
        filter.append(columnName).append(" IN (");

        for (int i = 0; i < values.length; i++) {
            if (i > 0) {
                filter.append(", ");
            }
            quoteStringLiteral(filter, values[i]);
        }

        filter.append(")");
        filters.add(filter.toString());
    }

    private static void emptyStringEqualsFilter(List<String> filters, String columnName, String value)
    {
        if (value != null) {
            if (value.isEmpty()) {
                filters.add(columnName + " IS NULL");
            }
            else {
                filters.add(stringColumnEquals(columnName, value));
            }
        }
    }

    private static void emptyStringLikeFilter(List<String> filters, String columnName, String value)
    {
        if (value != null) {
            if (value.isEmpty()) {
                filters.add(columnName + " IS NULL");
            }
            else {
                filters.add(stringColumnLike(columnName, value));
            }
        }
    }

    private static String stringColumnEquals(String columnName, String value)
    {
        StringBuilder filter = new StringBuilder();
        filter.append(columnName).append(" = ");
        quoteStringLiteral(filter, value);
        return filter.toString();
    }

    private static String stringColumnLike(String columnName, String pattern)
    {
        StringBuilder filter = new StringBuilder();
        filter.append(columnName).append(" LIKE ");
        quoteStringLiteral(filter, pattern);

        return filter.toString();
    }

    private static void quoteStringLiteral(StringBuilder out, String value)
    {
        out.append('\'');
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            out.append(c);
            if (c == '\'') {
                out.append('\'');
            }
        }
        out.append('\'');
    }

    private static StringBuilder columnMetaSqlTemplate() {
        StringBuilder sql = new StringBuilder("SELECT table_catalog as TABLE_CAT" +
                ", table_schema as TABLE_SCHEM" +
                ", table_name as TABLE_NAME" +
                ", column_name as COLUMN_NAME" +
                ", data_type as DATA_TYPE" +
                ", column_type as TYPE_NAME" +
                ", 0 as COLUMN_SIZE" +
                ", 0 as BUFFER_LENGTH" +
                ", 10 as DECIMAL_DIGITS" +
                ", 10 as NUM_PREC_RADIX" +
                ", column_comment as REMARKS" +
                ", column_default as COLUMN_DEF" +
                ", 0 as SQL_DATA_TYPE" +
                ", 0 as SQL_DATETIME_SUB" +
                ", 0 as CHAR_OCTET_LENGTH" +
                ", ordinal_position as ORDINAL_POSITION" +
                ", is_nullable as IS_NULLABLE" +
                ", nullable as NULLABLE" +
                ", null as SCOPE_CATALOG" +
                ", null as SCOPE_SCHEMA" +
                ", null as SCOPE_TABLE" +
                ", null as SOURCE_DATA_TYPE" +
                ", 'NO' as IS_AUTOINCREMENT" +
                ", 'NO' as IS_GENERATEDCOLUMN" +
                " FROM information_schema.columns");
        return sql;
    }

    @Override
    public boolean allProceduresAreCallable()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean allTablesAreSelectable()
            throws SQLException
    {
        return true;
    }

    @Override
    public String getURL()
            throws SQLException
    {
        return "jdbc:databend://" +  connection.getURI().toString();
    }

    @Override
    public String getUserName()
            throws SQLException
    {
        try (ResultSet rs = select("SELECT current_user()")) {
            if (rs.next()) {
                return rs.getString(1);
            }
        }
        return null;
    }

    @Override
    public boolean isReadOnly()
            throws SQLException
    {
        return getConnection().isReadOnly();
    }

    @Override
    public boolean nullsAreSortedHigh()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean nullsAreSortedLow()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean nullsAreSortedAtStart()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean nullsAreSortedAtEnd()
            throws SQLException
    {
        return false;
    }

    @Override
    public String getDatabaseProductName()
            throws SQLException
    {
        return "Databend";
    }

    @Override
    public String getDatabaseProductVersion()
            throws SQLException
    {
        try (ResultSet rs = select("SELECT version()")) {
            rs.next();
            return rs.getString(1);
        }
    }

    @Override
    public String getDriverName()
            throws SQLException
    {
        return DRIVER_NAME;
    }

    @Override
    public String getDriverVersion()
            throws SQLException
    {
        return DRIVER_VERSION;
    }

    @Override
    public int getDriverMajorVersion()
    {
        return DRIVER_VERSION_MAJOR;
    }

    @Override
    public int getDriverMinorVersion()
    {
        return DRIVER_VERSION_MINOR;
    }

    @Override
    public boolean usesLocalFiles()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean usesLocalFilePerTable()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean supportsMixedCaseIdentifiers()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean storesUpperCaseIdentifiers()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean storesLowerCaseIdentifiers()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean storesMixedCaseIdentifiers()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean supportsMixedCaseQuotedIdentifiers()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean storesUpperCaseQuotedIdentifiers()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean storesLowerCaseQuotedIdentifiers()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean storesMixedCaseQuotedIdentifiers()
            throws SQLException
    {
        return false;
    }

    @Override
    public String getIdentifierQuoteString()
            throws SQLException
    {
        return "\"";
    }

    @Override
    public String getSQLKeywords()
            throws SQLException
    {
        ArrayList<String> keywords = new ArrayList<>();
        try (ResultSet rs = select("SELECT word FROM information_schema.keywords")) {
            rs.next();
            keywords.add(rs.getString(1));
        }
        return String.join(",", keywords);
    }

    @Override
    public String getNumericFunctions()
            throws SQLException
    {
        // https://databend.rs/doc/reference/functions/numeric-functions
        return "abs,acos,asin,atan,atan2,ceil,cos,cot,degrees,e,exp,floor,ln,log,log10,mod,pi,power,radians,rand,round,sign,sin,sqrt,tan,truncate";
    }

    @Override
    public String getStringFunctions()
            throws SQLException
    {
        // https://databend.rs/doc/reference/functions/string-functions
        return "ascii,bin,bin_length,char,char_length,character_length,concat,concat_ws,elt,export_set,field,find_in_set,format,from_base64" +
                ",hex,insert,instr,lcase,left,length,like,locate,lower,lpad,mid,oct,octet_length,ord,position,quote,regexp,regexp_instr,regexp_like" +
                ",regexp_replace,regexp_substr,repeat,replace,reverse,right,rlike,rpad,soundex,space,strcmp,substr,substring,substring_index,to_base64,trim,ucase,unhex,upper";
    }

    @Override
    public String getSystemFunctions()
            throws SQLException
    {
        return "CLUSTERING_INFORMATION,FUSE_BLOCK,FUSE_SEGMENT,FUSE_SNAPSHOT,FUSE_STATISTIC";
    }

    @Override
    public String getTimeDateFunctions()
            throws SQLException
    {
        // https://databend.rs/doc/reference/functions/datetime-functions

        return "addDays,addHours,addMinutes,addMonths,addQuarters,addSeconds,addWeeks,addYears,date_add,date_diff,date_sub,date_trunc,dateName,formatDateTime,FROM_UNIXTIME,fromModifiedJulianDay,fromModifiedJulianDayOrNull,now,subtractDays,subtractHours,subtractMinutes,subtractMonths,subtractQuarters,subtractSeconds,subtractWeeks,subtractYears,timeSlot,timeSlots,timestamp_add,timestamp_sub,timeZone,timeZoneOf,timeZoneOffset,today,toDayOfMonth,toDayOfWeek,toDayOfYear,toHour,toISOWeek,toISOYear,toMinute,toModifiedJulianDay,toModifiedJulianDayOrNull,toMonday,toMonth,toQuarter,toRelativeDayNum,toRelativeHourNum,toRelativeMinuteNum,toRelativeMonthNum,toRelativeQuarterNum,toRelativeSecondNum,toRelativeWeekNum,toRelativeYearNum,toSecond,toStartOfDay,toStartOfFifteenMinutes,toStartOfFiveMinute,toStartOfHour,toStartOfInterval,toStartOfISOYear,toStartOfMinute,toStartOfMonth,toStartOfQuarter,toStartOfSecond,toStartOfTenMinutes,toStartOfWeek,toStartOfYear,toTime,toTimeZone,toUnixTimestamp,toWeek,toYear,toYearWeek,toYYYYMM,toYYYYMMDD,toYYYYMMDDhhmmss,yesterday";
    }

    @Override
    public String getSearchStringEscape()
            throws SQLException
    {
        return "\\";
    }

    @Override
    public String getExtraNameCharacters()
            throws SQLException
    {
        return "";
    }

    @Override
    public boolean supportsAlterTableWithAddColumn()
            throws SQLException
    {
        // https://github.com/datafuselabs/databend/issues/9441
        return true;
    }

    @Override
    public boolean supportsAlterTableWithDropColumn()
            throws SQLException
    {
        // https://github.com/datafuselabs/databend/issues/9441
        return true;
    }

    @Override
    public boolean supportsColumnAliasing()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean nullPlusNonNullIsNull()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsConvert()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean supportsConvert(int i, int i1)
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean supportsTableCorrelationNames()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean supportsDifferentTableCorrelationNames()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean supportsExpressionsInOrderBy()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsOrderByUnrelated()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsGroupBy()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsGroupByUnrelated()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsGroupByBeyondSelect()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsLikeEscapeClause()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsMultipleResultSets()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean supportsMultipleTransactions()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean supportsNonNullableColumns()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsMinimumSQLGrammar()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsCoreSQLGrammar()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsExtendedSQLGrammar()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsANSI92EntryLevelSQL()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsANSI92IntermediateSQL()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean supportsANSI92FullSQL()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean supportsIntegrityEnhancementFacility()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean supportsOuterJoins()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsFullOuterJoins()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsLimitedOuterJoins()
            throws SQLException
    {
        return true;
    }

    @Override
    public String getSchemaTerm()
            throws SQLException
    {
        return "database";
    }

    @Override
    public String getProcedureTerm()
            throws SQLException
    {
        return null;
    }

    @Override
    public String getCatalogTerm()
            throws SQLException
    {
        return null;
    }

    @Override
    public boolean isCatalogAtStart()
            throws SQLException
    {
        return false;
    }

    @Override
    public String getCatalogSeparator()
            throws SQLException
    {
        return null;
    }

    @Override
    public boolean supportsSchemasInDataManipulation()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean supportsSchemasInProcedureCalls()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean supportsSchemasInTableDefinitions()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean supportsSchemasInIndexDefinitions()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean supportsSchemasInPrivilegeDefinitions()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean supportsCatalogsInDataManipulation()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean supportsCatalogsInProcedureCalls()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean supportsCatalogsInTableDefinitions()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean supportsCatalogsInIndexDefinitions()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean supportsCatalogsInPrivilegeDefinitions()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean supportsPositionedDelete()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean supportsPositionedUpdate()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean supportsSelectForUpdate()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean supportsStoredProcedures()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInComparisons()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInExists()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInIns()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean supportsSubqueriesInQuantifieds()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean supportsCorrelatedSubqueries()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean supportsUnion()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean supportsUnionAll()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean supportsOpenCursorsAcrossRollback()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossCommit()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean supportsOpenStatementsAcrossRollback()
            throws SQLException
    {
        return false;
    }

    @Override
    public int getMaxBinaryLiteralLength()
            throws SQLException
    {
        return 0;
    }

    @Override
    public int getMaxCharLiteralLength()
            throws SQLException
    {
        return 0;
    }

    @Override
    public int getMaxColumnNameLength()
            throws SQLException
    {
        return 0;
    }

    @Override
    public int getMaxColumnsInGroupBy()
            throws SQLException
    {
        return 0;
    }

    @Override
    public int getMaxColumnsInIndex()
            throws SQLException
    {
        return 0;
    }

    @Override
    public int getMaxColumnsInOrderBy()
            throws SQLException
    {
        return 0;
    }

    @Override
    public int getMaxColumnsInSelect()
            throws SQLException
    {
        return 0;
    }

    @Override
    public int getMaxColumnsInTable()
            throws SQLException
    {
        return 0;
    }

    @Override
    public int getMaxConnections()
            throws SQLException
    {
        return 0;
    }

    @Override
    public int getMaxCursorNameLength()
            throws SQLException
    {
        return 0;
    }

    @Override
    public int getMaxIndexLength()
            throws SQLException
    {
        return 0;
    }

    @Override
    public int getMaxSchemaNameLength()
            throws SQLException
    {
        return 0;
    }

    @Override
    public int getMaxProcedureNameLength()
            throws SQLException
    {
        return 0;
    }

    @Override
    public int getMaxCatalogNameLength()
            throws SQLException
    {
        return 0;
    }

    @Override
    public int getMaxRowSize()
            throws SQLException
    {
        return 0;
    }

    @Override
    public boolean doesMaxRowSizeIncludeBlobs()
            throws SQLException
    {
        return false;
    }

    @Override
    public int getMaxStatementLength()
            throws SQLException
    {
        return 0;
    }

    @Override
    public int getMaxStatements()
            throws SQLException
    {
        return 0;
    }

    @Override
    public int getMaxTableNameLength()
            throws SQLException
    {
        return 0;
    }

    @Override
    public int getMaxTablesInSelect()
            throws SQLException
    {
        return 0;
    }

    @Override
    public int getMaxUserNameLength()
            throws SQLException
    {
        return 0;
    }

    @Override
    public int getDefaultTransactionIsolation()
            throws SQLException
    {
        return 0;
    }

    @Override
    public boolean supportsTransactions()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean supportsTransactionIsolationLevel(int i)
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean supportsDataDefinitionAndDataManipulationTransactions()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean supportsDataManipulationTransactionsOnly()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean dataDefinitionCausesTransactionCommit()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean dataDefinitionIgnoredInTransactions()
            throws SQLException
    {
        return false;
    }

    @Override
    public ResultSet getProcedures(String s, String s1, String s2)
            throws SQLException
    {
        return null;
    }

    @Override
    public ResultSet getProcedureColumns(String s, String s1, String s2, String s3)
            throws SQLException
    {
        return null;
    }

    @Override
    public ResultSet getTables(String catalog, String schemaPattern, String tableNamePattern, String[] types)
            throws SQLException
    {
        // getTables from information_schema.tables
        StringBuilder sql = new StringBuilder("SELECT table_catalog as TABLE_CAT" +
                ", table_schema as TABLE_SCHEM" +
                ", table_name as TABLE_NAME" +
                ", table_type as TABLE_TYPE" +
                ", table_comment as REMARKS" +
                ", '' as TYPE_CAT" +
                ", engine as TYPE_SCHEM" +
                ", engine as TYPE_NAME" +
                ", '' as SELF_REFERENCING_COL_NAME" +
                ", '' as REF_GENERATION" +
                " FROM information_schema.tables");
        List<String> filters = new ArrayList<>();
        emptyStringEqualsFilter(filters, "table_catalog", catalog);
        emptyStringLikeFilter(filters, "table_schema", schemaPattern);
        optionalStringLikeFilter(filters, "table_name", tableNamePattern);
        optionalStringInFilter(filters, "table_type", types);
        buildFilters(sql, filters);
        sql.append("\nORDER BY table_type, table_catalog, table_schema, table_name");

        return select(sql.toString());
    }

    @Override
    public ResultSet getSchemas()
            throws SQLException
    {
        String sql = "SELECT schema_name as table_schema, catalog_name as table_catalog FROM information_schema.schemata ORDER BY catalog_name, schema_name";
        return select(sql);
    }

    @Override
    public ResultSet getCatalogs()
            throws SQLException
    {
        String sql = "SELECT catalog_name as table_cat FROM information_schema.schemata ORDER BY catalog_name";
        return select(sql);
    }

    @Override
    public ResultSet getTableTypes()
            throws SQLException
    {
        return select("SELECT table_type as table_type FROM information_schema.tables GROUP BY table_type ORDER BY table_type");
    }

    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String[] columnNames) throws SQLException {
        StringBuilder sql = columnMetaSqlTemplate();
        List<String> filters = new ArrayList<>();
        emptyStringEqualsFilter(filters, "table_catalog", catalog);
        emptyStringLikeFilter(filters, "table_schema", schemaPattern);
        optionalStringLikeFilter(filters, "table_name", tableNamePattern);
        optionalStringInFilter(filters, "column_name", columnNames);
        buildFilters(sql, filters);
        sql.append("\nORDER BY table_catalog, table_schema, table_name, ordinal_position");
        return select(sql.toString());
    }

    @Override
    public ResultSet getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
            throws SQLException
    {
        StringBuilder sql = columnMetaSqlTemplate();
        List<String> filters = new ArrayList<>();
        emptyStringEqualsFilter(filters, "table_catalog", catalog);
        emptyStringLikeFilter(filters, "table_schema", schemaPattern);
        optionalStringLikeFilter(filters, "table_name", tableNamePattern);
        optionalStringLikeFilter(filters, "column_name", columnNamePattern);
        buildFilters(sql, filters);
        sql.append("\nORDER BY table_catalog, table_schema, table_name, ordinal_position");
        return select(sql.toString());
    }

    @Override
    public ResultSet getColumnPrivileges(String s, String s1, String s2, String s3)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("privileges not supported");
    }

    @Override
    public ResultSet getTablePrivileges(String s, String s1, String s2)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("privileges not supported");
    }

    @Override
    public ResultSet getBestRowIdentifier(String s, String s1, String s2, int i, boolean b)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("row identifiers not supported");
    }

    @Override
    public ResultSet getVersionColumns(String s, String s1, String s2)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("version columns not supported");
    }

    @Override
    public ResultSet getPrimaryKeys(String s, String s1, String s2)
            throws SQLException
    {
        String query = "SELECT " +
                " TRY_CAST(NULL AS varchar) table_cat, " +
                " TRY_CAST(NULL AS varchar) table_schema, " +
                " TRY_CAST(NULL AS varchar) table_name, " +
                " TRY_CAST(NULL AS varchar) column_name, " +
                " TRY_CAST(NULL AS smallint) key_seq, " +
                " TRY_CAST(NULL AS varchar) pk_name " +
                "WHERE false";
        return select(query);
    }

    @Override
    public ResultSet getImportedKeys(String s, String s1, String s2)
            throws SQLException
    {
        String query = "SELECT " +
                " TRY_CAST(NULL AS varchar) PKTABLE_CAT, " +
                " TRY_CAST(NULL AS varchar) PKTABLE_SCHEM, " +
                " TRY_CAST(NULL AS varchar) PKTABLE_NAME, " +
                " TRY_CAST(NULL AS varchar) PKCOLUMN_NAME, " +
                " TRY_CAST(NULL AS varchar) FKTABLE_CAT, " +
                " TRY_CAST(NULL AS varchar) FKTABLE_SCHEM, " +
                " TRY_CAST(NULL AS varchar) FKTABLE_NAME, " +
                " TRY_CAST(NULL AS varchar) FKCOLUMN_NAME, " +
                " TRY_CAST(NULL AS smallint) KEY_SEQ, " +
                " TRY_CAST(NULL AS smallint) UPDATE_RULE, " +
                " TRY_CAST(NULL AS smallint) DELETE_RULE, " +
                " TRY_CAST(NULL AS varchar) FK_NAME, " +
                " TRY_CAST(NULL AS varchar) PK_NAME, " +
                " TRY_CAST(NULL AS smallint) DEFERRABILITY " +
                "WHERE false";
        return select(query);
    }

    @Override
    public ResultSet getExportedKeys(String s, String s1, String s2)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("exported keys not supported");
    }

    @Override
    public ResultSet getCrossReference(String s, String s1, String s2, String s3, String s4, String s5)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("cross reference not supported");
    }

    @Override
    public ResultSet getTypeInfo()
            throws SQLException
    {
        return select("SELECT " +
                " TRY_CAST(NULL AS varchar) TYPE_NAME, " +
                " TRY_CAST(NULL AS smallint) DATA_TYPE, " +
                " TRY_CAST(NULL AS int) PRECISION, " +
                " TRY_CAST(NULL AS varchar) LITERAL_PREFIX, " +
                " TRY_CAST(NULL AS varchar) LITERAL_SUFFIX, " +
                " TRY_CAST(NULL AS varchar) CREATE_PARAMS, " +
                " TRY_CAST(NULL AS smallint) NULLABLE, " +
                " TRY_CAST(NULL AS boolean) CASE_SENSITIVE, " +
                " TRY_CAST(NULL AS smallint) SEARCHABLE, " +
                " TRY_CAST(NULL AS boolean) UNSIGNED_ATTRIBUTE, " +
                " TRY_CAST(NULL AS boolean) FIXED_PREC_SCALE, " +
                " TRY_CAST(NULL AS boolean) AUTO_INCREMENT, " +
                " TRY_CAST(NULL AS varchar) LOCAL_TYPE_NAME, " +
                " TRY_CAST(NULL AS smallint) MINIMUM_SCALE, " +
                " TRY_CAST(NULL AS smallint) MAXIMUM_SCALE, " +
                " TRY_CAST(NULL AS int) SQL_DATA_TYPE, " +
                " TRY_CAST(NULL AS int) SQL_DATETIME_SUB, " +
                " TRY_CAST(NULL AS int) NUM_PREC_RADIX " +
                "WHERE false");
    }

    @Override
    public ResultSet getIndexInfo(String s, String s1, String s2, boolean b, boolean b1)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("index info not supported");
    }

    @Override
    public boolean supportsResultSetType(int type)
            throws SQLException
    {
        return type == ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public boolean supportsResultSetConcurrency(int type, int concurrency)
            throws SQLException
    {
        return (type == ResultSet.TYPE_FORWARD_ONLY) &&
                (concurrency == ResultSet.CONCUR_READ_ONLY);
    }

    @Override
    public boolean ownUpdatesAreVisible(int i)
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean ownDeletesAreVisible(int i)
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean ownInsertsAreVisible(int i)
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean othersUpdatesAreVisible(int i)
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean othersDeletesAreVisible(int i)
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean othersInsertsAreVisible(int i)
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean updatesAreDetected(int i)
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean deletesAreDetected(int i)
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean insertsAreDetected(int i)
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean supportsBatchUpdates()
            throws SQLException
    {
        return true;
    }

    @Override
    public ResultSet getUDTs(String s, String s1, String s2, int[] ints)
            throws SQLException
    {
        return select("SELECT " +
                " TRY_CAST(NULL AS varchar) TYPE_CAT, " +
                " TRY_CAST(NULL AS varchar) TYPE_SCHEM, " +
                " TRY_CAST(NULL AS varchar) TYPE_NAME, " +
                " TRY_CAST(NULL AS varchar) CLASS_NAME, " +
                " TRY_CAST(NULL AS smallint) DATA_TYPE, " +
                " TRY_CAST(NULL AS varchar) REMARKS, " +
                " TRY_CAST(NULL AS smallint) BASE_TYPE " +
                "WHERE false");
    }

    @Override
    public Connection getConnection()
            throws SQLException
    {
        return connection;
    }

    @Override
    public boolean supportsSavepoints()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean supportsNamedParameters()
            throws SQLException
    {
        return true;
    }

    @Override
    public boolean supportsMultipleOpenResults()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean supportsGetGeneratedKeys()
            throws SQLException
    {
        return false;
    }

    @Override
    public ResultSet getSuperTypes(String s, String s1, String s2)
            throws SQLException
    {
        return select("SELECT " +
                " CAST(NULL AS varchar) TYPE_CAT, " +
                " CAST(NULL AS varchar) TYPE_SCHEM, " +
                " CAST(NULL AS varchar) TYPE_NAME, " +
                " CAST(NULL AS varchar) SUPERTYPE_CAT, " +
                " CAST(NULL AS varchar) SUPERTYPE_SCHEM, " +
                " CAST(NULL AS varchar) SUPERTYPE_NAME " +
                "WHERE false");
    }

    @Override
    public ResultSet getSuperTables(String s, String s1, String s2)
            throws SQLException
    {
        return select("SELECT " +
                " CAST(NULL AS varchar) TABLE_CAT, " +
                " CAST(NULL AS varchar) TABLE_SCHEM, " +
                " CAST(NULL AS varchar) TABLE_NAME, " +
                " CAST(NULL AS varchar) SUPERTABLE_NAME " +
                "WHERE false");
    }

    @Override
    public ResultSet getAttributes(String s, String s1, String s2, String s3)
            throws SQLException
    {
        return select("SELECT " +
                " TRY_CAST(NULL AS varchar) TYPE_CAT, " +
                " TRY_CAST(NULL AS varchar) TYPE_SCHEM, " +
                " TRY_CAST(NULL AS varchar) TYPE_NAME, " +
                " TRY_CAST(NULL AS varchar) ATTR_NAME, " +
                " TRY_CAST(NULL AS int) DATA_TYPE, " +
                " TRY_CAST(NULL AS varchar) ATTR_TYPE_NAME, " +
                " TRY_CAST(NULL AS int) ATTR_SIZE, " +
                " TRY_CAST(NULL AS int) DECIMAL_DIGITS, " +
                " TRY_CAST(NULL AS int) NUM_PREC_RADIX, " +
                " TRY_CAST(NULL AS smallint) NULLABLE, " +
                " TRY_CAST(NULL AS varchar) REMARKS, " +
                " TRY_CAST(NULL AS varchar) ATTR_DEF, " +
                " TRY_CAST(NULL AS int) SQL_DATA_TYPE, " +
                " TRY_CAST(NULL AS int) SQL_DATETIME_SUB, " +
                " TRY_CAST(NULL AS int) CHAR_OCTET_LENGTH, " +
                " TRY_CAST(NULL AS int) ORDINAL_POSITION, " +
                " TRY_CAST(NULL AS varchar) IS_NULLABLE, " +
                " TRY_CAST(NULL AS varchar) SCOPE_CATALOG, " +
                " TRY_CAST(NULL AS varchar) SCOPE_SCHEMA, " +
                " TRY_CAST(NULL AS varchar) SCOPE_TABLE, " +
                " TRY_CAST(NULL AS smallint) SOURCE_DATA_TYPE " +
                "WHERE false");
    }

    @Override
    public boolean supportsResultSetHoldability(int holdability) throws SQLException {
        return false;
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        // N/A applicable as we do not support transactions
        return 0;
    }

    // input DatabendQuery v0.8.173-nightly-d66d905(rust-1.67.0-nightly-2023-01-03T08:02:54.266305248Z)
    // return 8 use regex
    @Override
    public int getDatabaseMajorVersion()
            throws SQLException
    {
        String version = getDatabaseProductVersion();
        // split by empty space
        String[] versionArray = version.split(" ");
        if (versionArray.length < 2) {
            return -1;
        }
        // regex matching v%d.%d.%d
        Pattern pattern = Pattern.compile("v(\\d+)\\.(\\d+)\\.(\\d+)");
        Matcher matcher = pattern.matcher(versionArray[1]);
        if (matcher.find()) {
            return 10 * Integer.parseInt(matcher.group(1)) + Integer.parseInt(matcher.group(2));
        }
        return -1;
    }

    // return 173
    @Override
    public int getDatabaseMinorVersion()
            throws SQLException
    {
        String version = getDatabaseProductVersion();
        // split by empty space
        String[] versionArray = version.split(" ");
        if (versionArray.length < 2) {
            return -1;
        }
        // regex matching v%d.%d.%d
        Pattern pattern = Pattern.compile("v(\\d+)\\.(\\d+)\\.(\\d+)");
        Matcher matcher = pattern.matcher(versionArray[1]);
        if (matcher.find()) {
            return Integer.parseInt(matcher.group(3));
        }
        return -1;
    }

    @Override
    public int getJDBCMajorVersion()
            throws SQLException
    {
        return 0;
    }

    @Override
    public int getJDBCMinorVersion()
            throws SQLException
    {
        return 1;
    }

    @Override
    public int getSQLStateType()
            throws SQLException
    {
        return DatabaseMetaData.sqlStateSQL99;
    }

    @Override
    public boolean locatorsUpdateCopy()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean supportsStatementPooling()
            throws SQLException
    {
        return false;
    }

    @Override
    public RowIdLifetime getRowIdLifetime()
            throws SQLException
    {
        return RowIdLifetime.ROWID_UNSUPPORTED;
    }

    @Override
    public ResultSet getSchemas(String catalog, String schemaPattern)
            throws SQLException
    {
        // from information schema
        StringBuilder sql = new StringBuilder("SELECT " +
                "schema_name as TABLE_SCHEM, " +
                "catalog_name as TABLE_CATALOG " +
                "FROM information_schema.schemata ");
        List<String> filters = new ArrayList<>();
        emptyStringEqualsFilter(filters, "catalog_name", catalog);
        emptyStringEqualsFilter(filters, "schema_name", schemaPattern);
        buildFilters(sql, filters);
        sql.append("\n ORDER BY catalog_name, schema_name");
        return select(sql.toString());
    }

    @Override
    public boolean supportsStoredFunctionsUsingCallSyntax()
            throws SQLException
    {
        return false;
    }

    @Override
    public boolean autoCommitFailureClosesAllResultSets()
            throws SQLException
    {
        return false;
    }

    @Override
    public ResultSet getClientInfoProperties()
            throws SQLException
    {
        return select("SELECT " +
                " TRY_CAST(NULL AS varchar) NAME, " +
                " TRY_CAST(NULL AS varchar) MAX_LEN, " +
                " TRY_CAST(NULL AS varchar) DEFAULT_VALUE, " +
                " TRY_CAST(NULL AS varchar) DESCRIPTION " +
                "WHERE false");
    }

    @Override
    public ResultSet getFunctions(String catalog, String schemaPattern, String functionNamePattern)
            throws SQLException
    {
        StringBuilder sql = new StringBuilder("SELECT " +
                " current_database() as FUNCTION_CAT, " +
                " 'system' as FUNCTION_SCHEMA, " +
                " name as FUNCTION_NAME, " +
                " description as REMARKS, " +
                " 1 as FUNCTION_TYPE, " +
                " name as SPECIFIC_NAME" +
                "FROM system.functions");
        List<String> filters = new ArrayList<>();
        optionalStringLikeFilter(filters, "function_name", functionNamePattern);
        buildFilters(sql, filters);
        sql.append("\n ORDER BY function_cat, function_schema, function_name");
        return select(sql.toString());
    }

    @Override
    public ResultSet getFunctionColumns(String catalog, String schemaPattern, String functionNamePattern, String columnNamePattern)
            throws SQLException
    {
        StringBuilder sql = new StringBuilder("SELECT " +
                " current_database() as FUNCTION_CAT, " +
                " 'system' as FUNCTION_SCHEMA, " +
                " name as FUNCTION_NAME, " +
                " TRY_CAST(NULL AS varchar) COLUMN_NAME, " +
                " TRY_CAST(NULL AS smallint) COLUMN_TYPE, " +
                " TRY_CAST(NULL AS smallint) DATA_TYPE, " +
                " TRY_CAST(NULL AS varchar) TYPE_NAME, " +
                " TRY_CAST(NULL AS int) PRECISION, " +
                " TRY_CAST(NULL AS int) LENGTH, " +
                " TRY_CAST(NULL AS int) SCALE, " +
                " TRY_CAST(NULL AS int) RADIX, " +
                " TRY_CAST(NULL AS smallint) NULLABLE, " +
                " TRY_CAST(NULL AS varchar) REMARKS, " +
                " TRY_CAST(NULL AS varchar) CHAR_OCTET_LENGTH, " +
                " TRY_CAST(NULL AS int) ORDINAL_POSITION, " +
                " TRY_CAST(NULL AS varchar) IS_NULLABLE, " +
                " TRY_CAST(NULL AS varchar) SPECIFIC_NAME " +
                "FROM system.functions");
        List<String> filters = new ArrayList<>();
        optionalStringLikeFilter(filters, "function_name", functionNamePattern);
        buildFilters(sql, filters);
        sql.append("\n ORDER BY function_cat, function_schema, function_name");
        return select(sql.toString());
    }

    @Override
    public ResultSet getPseudoColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
            throws SQLException
    {
        return select("SELECT " +
                " TRY_CAST(NULL AS varchar) TABLE_CAT, " +
                " TRY_CAST(NULL AS varchar) TABLE_SCHEM, " +
                " TRY_CAST(NULL AS varchar) TABLE_NAME, " +
                " TRY_CAST(NULL AS varchar) COLUMN_NAME, " +
                " TRY_CAST(NULL AS smallint) DATA_TYPE, " +
                " TRY_CAST(NULL AS varchar) COLUMN_SIZE, " +
                " TRY_CAST(NULL AS int) DECIMAL_DIGITS, " +
                " TRY_CAST(NULL AS int) NUM_PREC_RADIX, " +
                " TRY_CAST(NULL AS smallint) COLUMN_USAGE, " +
                " TRY_CAST(NULL AS varchar) REMARKS, " +
                " TRY_CAST(NULL AS varchar) CHAR_OCTET_LENGTH, " +
                " TRY_CAST(NULL AS int) ORDINAL_POSITION, " +
                " TRY_CAST(NULL AS varchar) IS_NULLABLE, " +
                " TRY_CAST(NULL AS varchar) SCOPE_CATALOG, " +
                " TRY_CAST(NULL AS varchar) SCOPE_SCHEMA, " +
                " TRY_CAST(NULL AS varchar) SCOPE_TABLE, " +
                " TRY_CAST(NULL AS smallint) SOURCE_DATA_TYPE " +
                "WHERE false");
    }

    @Override
    public boolean generatedKeyAlwaysReturned()
            throws SQLException
    {
        return false;
    }

    @Override
    public <T> T unwrap(Class<T> iface)
            throws SQLException
    {
        if (isWrapperFor(iface)) {
            return (T) this;
        }
        throw new SQLException("No wrapper for " + iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface)
            throws SQLException
    {
        return iface.isInstance(this);
    }

    private ResultSet select(String sql)
            throws SQLException
    {
        Statement statement = getConnection().createStatement();
        DatabendResultSet resultSet;
        try {
            resultSet = (DatabendResultSet) statement.executeQuery(sql);
        }
        catch (Throwable e) {
            try {
                statement.close();
            }
            catch (Throwable closeException) {
                if (closeException != e) {
                    e.addSuppressed(closeException);
                }
            }

            throw e;
        }
        return (ResultSet) resultSet;
    }
}
