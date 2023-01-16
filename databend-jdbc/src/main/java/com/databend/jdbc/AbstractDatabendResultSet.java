package com.databend.jdbc;

import com.databend.client.QueryResults;
import com.databend.client.QueryRowField;
import com.databend.client.QuerySchema;
import com.databend.client.errors.QueryErrors;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.joda.time.DateTimeZone;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLType;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.databend.jdbc.ColumnInfo.setTypeInfo;
import static java.lang.String.format;
import static java.math.RoundingMode.HALF_UP;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

abstract class AbstractDatabendResultSet implements ResultSet
{
    protected final Iterator<List<Object>> results;
    protected final QuerySchema schema;
    private final Optional<Statement> statement;
    private final AtomicReference<List<Object>> row = new AtomicReference<>();
    private final AtomicLong currentRowNumber = new AtomicLong(); // Index into 'rows' of our current row (1-based)
    private final AtomicBoolean wasNull = new AtomicBoolean();
    private final Map<String, Integer> fieldMap;
    private final List<ColumnInfo> columnInfoList;

    private final ResultSetMetaData resultSetMetaData;

    AbstractDatabendResultSet(Optional<Statement> statement, QuerySchema schema, Iterator<List<Object>> results)
    {
        this.statement = requireNonNull(statement, "statement is null");
        this.schema =  requireNonNull(schema, "schema is null");;
        this.fieldMap = getFieldMap(schema.getFields());
        this.columnInfoList = getColumnInfo(schema.getFields());
        this.results = requireNonNull(results, "results is null");
        this.resultSetMetaData = new DatabendResultSetMetaData(columnInfoList);
    }

    private static Map<String, Integer> getFieldMap(List<QueryRowField> columns)
    {
        Map<String, Integer> map = Maps.newHashMapWithExpectedSize(columns.size());
        for (int i = 0; i < columns.size(); i++) {
            String name = columns.get(i).getName().toLowerCase(ENGLISH);
            if (!map.containsKey(name)) {
                map.put(name, i + 1);
            }
        }
        return ImmutableMap.copyOf(map);
    }

    private static List<ColumnInfo> getColumnInfo(List<QueryRowField> columns)
    {
        ImmutableList.Builder<ColumnInfo> list = ImmutableList.builderWithExpectedSize(columns.size());
        for (QueryRowField column : columns) {
            ColumnInfo.Builder builder = new ColumnInfo.Builder()
                    .setCatalogName("") // TODO
                    .setSchemaName("") // TODO
                    .setTableName("") // TODO
                    .setColumnLabel(column.getName())
                    .setColumnName(column.getName()) // TODO
                    .setColumnTypeSignature(column.getDataType())
                    .setCurrency(false);
            setTypeInfo(builder, column.getDataType());
            list.add(builder.build());
        }
        return list.build();
    }

    private static Optional<BigDecimal> toBigDecimal(String value)
    {
        try {
            return Optional.of(new BigDecimal(value));
        }
        catch (NumberFormatException ne) {
            return Optional.empty();
        }
    }

    private static BigDecimal parseBigDecimal(String value)
            throws SQLException
    {
        return toBigDecimal(String.valueOf(value))
                .orElseThrow(() -> new SQLException("Value is not a number: " + value));
    }

    static SQLException resultsException(QueryResults results)
    {
        QueryErrors error = requireNonNull(results.getError());
        String message = format("Query failed (#%s): %s", results.getId(), error.getMessage());
        return new SQLException(message, String.valueOf(error.getCode()));
    }

    private void checkOpen()
            throws SQLException
    {
        if (isClosed()) {
            throw new SQLException("ResultSet is closed");
        }
    }

    @Override
    public boolean next() throws SQLException
    {
        checkOpen();
        try {
            if (!results.hasNext()) {
                row.set(null);
                currentRowNumber.set(0);
                return false;
            }
            row.set(results.next());
            currentRowNumber.incrementAndGet();
            return true;
        }
        catch (RuntimeException e) {
            if (e.getCause() instanceof SQLException) {
                throw (SQLException) e.getCause();
            }
            throw new SQLException("error fetching results", e);
        }
    }

    @Override
    public boolean wasNull() throws SQLException
    {
        return wasNull.get();
    }

    private void checkValidRow()
            throws SQLException
    {
        if (row.get() == null) {
            throw new SQLException("Not on a valid row");
        }
    }

    private Object column(int index)
            throws SQLException
    {
        checkOpen();
        checkValidRow();
        if ((index <= 0) || (index > resultSetMetaData.getColumnCount())) {
            throw new SQLException("Invalid column index: " + index);
        }
        Object value = row.get().get(index - 1);
        wasNull.set(value == null);
        return value;
    }

    @Override
    public String getString(int columnIndex)
            throws SQLException
    {
        Object value = column(columnIndex);
        if (value == null) {
            return null;
        }
        return value.toString();
    }

    @Override
    public boolean getBoolean(int columnIndex)
            throws SQLException
    {
        Object value = column(columnIndex);
        if (value == null) {
            return false;
        }
        return (Boolean) value;
    }

    @Override
    public byte getByte(int columnIndex)
            throws SQLException
    {
        Object value = column(columnIndex);
        if (value == null) {
            return 0;
        }
        return ((Number) value).byteValue();
    }

    @Override
    public short getShort(int columnIndex)
            throws SQLException
    {
        Object value = column(columnIndex);
        if (value == null) {
            return 0;
        }
        return ((Number) value).shortValue();
    }

    @Override
    public int getInt(int columnIndex)
            throws SQLException
    {
        Object value = column(columnIndex);
        if (value == null) {
            return 0;
        }
        return ((Number) value).intValue();
    }

    @Override
    public long getLong(int columnIndex)
            throws SQLException
    {
        Object value = column(columnIndex);
        if (value == null) {
            return 0;
        }
        return ((Number) value).longValue();
    }

    @Override
    public float getFloat(int columnIndex)
            throws SQLException
    {
        Object value = column(columnIndex);
        if (value == null) {
            return 0;
        }
        return ((Number) value).floatValue();
    }

    @Override
    public double getDouble(int columnIndex)
            throws SQLException
    {
        Object value = column(columnIndex);
        if (value == null) {
            return 0;
        }
        return ((Number) value).doubleValue();
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale)
            throws SQLException
    {
        BigDecimal bigDecimal = getBigDecimal(columnIndex);
        if (bigDecimal != null) {
            bigDecimal = bigDecimal.setScale(scale, HALF_UP);
        }
        return bigDecimal;
    }

    @Override
    public byte[] getBytes(int columnIndex)
            throws SQLException
    {
        final Object value = column(columnIndex);
        if (value == null) {
            return null;
        }
        if (value instanceof byte[]) {
            return (byte[]) value;
        }
        throw new SQLException("Value is not a byte array: " + value);
    }

    @Override
    public Date getDate(int columnIndex)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("getDate");
    }

    @Override
    public Time getTime(int columnIndex)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("getTime");
    }

    @Override
    public Timestamp getTimestamp(int columnIndex)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("getTimestamp");
    }

    @Override
    public InputStream getAsciiStream(int columnIndex)
            throws SQLException
    {
        Object value = column(columnIndex);
        if (value == null) {
            return null;
        }
        if (!(value instanceof String)) {
            throw new SQLException("Value is not a string: " + value);
        }
        // TODO: a stream returned here should get implicitly closed
        //  on any subsequent invocation of a ResultSet getter method.
        return new ByteArrayInputStream(((String) value).getBytes(StandardCharsets.US_ASCII));
    }

    @Override
    public InputStream getUnicodeStream(int columnIndex)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("getUnicodeStream");
    }

    @Override
    public InputStream getBinaryStream(int columnIndex)
            throws SQLException
    {
        byte[] value = getBytes(columnIndex);
        if (value == null) {
            return null;
        }
        // TODO: a stream returned here should get implicitly closed
        //  on any subsequent invocation of a ResultSet getter method.
        return new ByteArrayInputStream(value);
    }

    private int columnIndex(String label)
            throws SQLException
    {
        if (label == null) {
            throw new SQLException("Column label is null");
        }
        Integer index = fieldMap.get(label.toLowerCase(ENGLISH));
        if (index == null) {
            throw new SQLException("Invalid column label: " + label);
        }
        return index;
    }

    @Override
    public String getString(String columnLabel)
            throws SQLException
    {
        return getString(columnIndex(columnLabel));
    }

    @Override
    public boolean getBoolean(String columnLabel)
            throws SQLException
    {
        return getBoolean(columnIndex(columnLabel));
    }

    @Override
    public byte getByte(String columnLabel)
            throws SQLException
    {
        return getByte(columnIndex(columnLabel));
    }

    @Override
    public short getShort(String columnLabel)
            throws SQLException
    {
        return getShort(columnIndex(columnLabel));
    }

    @Override
    public int getInt(String columnLabel)
            throws SQLException
    {
        return getInt(columnIndex(columnLabel));
    }

    @Override
    public long getLong(String columnLabel)
            throws SQLException
    {
        return getLong(columnIndex(columnLabel));
    }

    @Override
    public float getFloat(String columnLabel)
            throws SQLException
    {
        return getFloat(columnIndex(columnLabel));
    }

    @Override
    public double getDouble(String columnLabel)
            throws SQLException
    {
        return getDouble(columnIndex(columnLabel));
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale)
            throws SQLException
    {
        return getBigDecimal(columnIndex(columnLabel), scale);
    }

    @Override
    public byte[] getBytes(String columnLabel)
            throws SQLException
    {
        return getBytes(columnIndex(columnLabel));
    }

    @Override
    public Date getDate(String columnLabel)
            throws SQLException
    {
        return getDate(columnIndex(columnLabel));
    }

    @Override
    public Time getTime(String columnLabel)
            throws SQLException
    {
        return getTime(columnIndex(columnLabel));
    }

    @Override
    public Timestamp getTimestamp(String columnLabel)
            throws SQLException
    {
        return getTimestamp(columnIndex(columnLabel));
    }

    @Override
    public InputStream getAsciiStream(String columnLabel)
            throws SQLException
    {
        return getAsciiStream(columnIndex(columnLabel));
    }

    @Override
    public InputStream getUnicodeStream(String columnLabel)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("getUnicodeStream");
    }

    @Override
    public InputStream getBinaryStream(String columnLabel)
            throws SQLException
    {
        return getBinaryStream(columnIndex(columnLabel));
    }

    @Override
    public SQLWarning getWarnings()
            throws SQLException
    {
        checkOpen();
        return null;
    }

    @Override
    public void clearWarnings()
            throws SQLException
    {
        checkOpen();
    }

    @Override
    public String getCursorName()
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("getCursorName");
    }

    @Override
    public ResultSetMetaData getMetaData()
            throws SQLException
    {
        return resultSetMetaData;
    }

    @Override
    public Object getObject(int columnIndex)
            throws SQLException
    {
        return column(columnIndex);
    }

    @Override
    public Object getObject(String columnLabel)
            throws SQLException
    {
        return getObject(columnIndex(columnLabel));
    }

    @Override
    public int findColumn(String columnLabel)
            throws SQLException
    {
        checkOpen();
        return columnIndex(columnLabel);
    }

    @Override
    public Reader getCharacterStream(int columnIndex)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet", "getCharacterStream");
    }

    @Override
    public Reader getCharacterStream(String columnLabel)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("ResultSet", "getCharacterStream");
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex)
            throws SQLException
    {
        Object value = column(columnIndex);
        if (value == null) {
            return null;
        }

        return parseBigDecimal(String.valueOf(value));
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel)
            throws SQLException
    {
        return getBigDecimal(columnIndex(columnLabel));
    }

    @Override
    public boolean isBeforeFirst()
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("isBeforeFirst");
    }

    @Override
    public boolean isAfterLast()
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("isAfterLast");
    }

    @Override
    public boolean isFirst()
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("isFirst");
    }

    @Override
    public boolean isLast()
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("isLast");
    }

    @Override
    public void beforeFirst()
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("beforeFirst");
    }

    @Override
    public void afterLast()
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("afterLast");
    }

    @Override
    public boolean first()
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("first");
    }

    @Override
    public boolean last()
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("last");
    }

    @Override
    public int getRow()
            throws SQLException
    {
        checkOpen();

        long rowNumber = currentRowNumber.get();
        if (rowNumber < 0 || rowNumber > Integer.MAX_VALUE) {
            throw new SQLException("Current row exceeds limit of 2147483647");
        }

        return (int) rowNumber;
    }

    @Override
    public boolean absolute(int row)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("absolute");
    }

    @Override
    public boolean relative(int rows)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("relative");
    }

    @Override
    public boolean previous()
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("previous");
    }

    @Override
    public int getFetchDirection()
            throws SQLException
    {
        checkOpen();
        return FETCH_FORWARD;
    }

    @Override
    public void setFetchDirection(int direction)
            throws SQLException
    {
        checkOpen();
        if (direction != FETCH_FORWARD) {
            throw new SQLException("Fetch direction must be FETCH_FORWARD");
        }
    }

    @Override
    public int getFetchSize()
            throws SQLException
    {
        checkOpen();
        // fetch size is ignored
        return 0;
    }

    @Override
    public void setFetchSize(int rows)
            throws SQLException
    {
        checkOpen();
        if (rows < 0) {
            throw new SQLException("Rows is negative");
        }
        // fetch size is ignored
    }

    @Override
    public int getType()
            throws SQLException
    {
        checkOpen();
        return TYPE_FORWARD_ONLY;
    }

    @Override
    public int getConcurrency()
            throws SQLException
    {
        checkOpen();
        return CONCUR_READ_ONLY;
    }

    @Override
    public boolean rowUpdated()
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("rowUpdated");
    }

    @Override
    public boolean rowInserted()
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("rowInserted");
    }

    @Override
    public boolean rowDeleted()
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("rowDeleted");
    }

    @Override
    public void updateNull(int columnIndex)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateNull");
    }

    @Override
    public void updateBoolean(int columnIndex, boolean x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateBoolean");
    }

    @Override
    public void updateByte(int columnIndex, byte x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateByte");
    }

    @Override
    public void updateShort(int columnIndex, short x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateShort");
    }

    @Override
    public void updateInt(int columnIndex, int x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateInt");
    }

    @Override
    public void updateLong(int columnIndex, long x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateLong");
    }

    @Override
    public void updateFloat(int columnIndex, float x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateFloat");
    }

    @Override
    public void updateDouble(int columnIndex, double x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateDouble");
    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateBigDecimal");
    }

    @Override
    public void updateString(int columnIndex, String x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateString");
    }

    @Override
    public void updateBytes(int columnIndex, byte[] x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateBytes");
    }

    @Override
    public void updateDate(int columnIndex, Date x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateDate");
    }

    @Override
    public void updateTime(int columnIndex, Time x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateTime");
    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateTimestamp");
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateAsciiStream");
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateBinaryStream");
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateCharacterStream");
    }

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateObject");
    }

    @Override
    public void updateObject(int columnIndex, Object x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateObject");
    }

    @Override
    public void updateNull(String columnLabel)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateNull");
    }

    @Override
    public void updateObject(int columnIndex, Object x, SQLType targetSqlType, int scaleOrLength)
            throws SQLException
    {
        this.updateObject(columnIndex, x, targetSqlType, scaleOrLength);
    }

    @Override
    public void updateObject(String columnLabel, Object x, SQLType targetSqlType, int scaleOrLength)
            throws SQLException
    {
        this.updateObject(columnLabel, x, targetSqlType, scaleOrLength);
    }

    @Override
    public void updateObject(int columnIndex, Object x, SQLType targetSqlType)
            throws SQLException
    {
        this.updateObject(columnIndex, x, targetSqlType);
    }

    @Override
    public void updateObject(String columnLabel, Object x, SQLType targetSqlType)
            throws SQLException
    {
        this.updateObject(columnLabel, x, targetSqlType);
    }

    @Override
    public void updateBoolean(String columnLabel, boolean x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateBoolean");
    }

    @Override
    public void updateByte(String columnLabel, byte x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateByte");
    }

    @Override
    public void updateShort(String columnLabel, short x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateShort");
    }

    @Override
    public void updateInt(String columnLabel, int x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateInt");
    }

    @Override
    public void updateLong(String columnLabel, long x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateLong");
    }

    @Override
    public void updateFloat(String columnLabel, float x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateFloat");
    }

    @Override
    public void updateDouble(String columnLabel, double x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateDouble");
    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateBigDecimal");
    }

    @Override
    public void updateString(String columnLabel, String x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateString");
    }

    @Override
    public void updateBytes(String columnLabel, byte[] x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateBytes");
    }

    @Override
    public void updateDate(String columnLabel, Date x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateDate");
    }

    @Override
    public void updateTime(String columnLabel, Time x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateTime");
    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateTimestamp");
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateAsciiStream");
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateBinaryStream");
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateCharacterStream");
    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateObject");
    }

    @Override
    public void updateObject(String columnLabel, Object x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateObject");
    }

    @Override
    public void insertRow()
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("insertRow");
    }

    @Override
    public void updateRow()
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateRow");
    }

    @Override
    public void deleteRow()
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("deleteRow");
    }

    @Override
    public void refreshRow()
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("refreshRow");
    }

    @Override
    public void cancelRowUpdates()
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("cancelRowUpdates");
    }

    @Override
    public void moveToInsertRow()
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("moveToInsertRow");
    }

    @Override
    public void moveToCurrentRow()
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("moveToCurrentRow");
    }

    @Override
    public Statement getStatement()
            throws SQLException
    {
        if (statement.isPresent()) {
            return statement.get();
        }

        throw new SQLException("Statement not available");
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("getObject");
    }

    @Override
    public Ref getRef(int columnIndex)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("getRef");
    }

    @Override
    public Blob getBlob(int columnIndex)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("getBlob");
    }

    @Override
    public Clob getClob(int columnIndex)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("getClob");
    }

    @Override
    public Array getArray(int columnIndex)
            throws SQLException
    {
        // TODO support it
        throw new SQLFeatureNotSupportedException("getArray");
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("getObject");
    }

    @Override
    public Ref getRef(String columnLabel)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("getRef");
    }

    @Override
    public Blob getBlob(String columnLabel)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("getBlob");
    }

    @Override
    public Clob getClob(String columnLabel)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("getClob");
    }

    @Override
    public Array getArray(String columnLabel)
            throws SQLException
    {
        return getArray(columnIndex(columnLabel));
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("getDate");
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal)
            throws SQLException
    {
        return getDate(columnIndex(columnLabel), cal);
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("getTime");
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal)
            throws SQLException
    {
        return getTime(columnIndex(columnLabel), cal);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("getTimestamp");
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal)
            throws SQLException
    {
        return getTimestamp(columnIndex(columnLabel), cal);
    }

    @Override
    public URL getURL(int columnIndex)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("getURL");
    }

    @Override
    public URL getURL(String columnLabel)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("getURL");
    }

    @Override
    public void updateRef(int columnIndex, Ref x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateRef");
    }

    @Override
    public void updateRef(String columnLabel, Ref x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateRef");
    }

    @Override
    public void updateBlob(int columnIndex, Blob x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateBlob");
    }

    @Override
    public void updateBlob(String columnLabel, Blob x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateBlob");
    }

    @Override
    public void updateClob(int columnIndex, Clob x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateClob");
    }

    @Override
    public void updateClob(String columnLabel, Clob x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateClob");
    }

    @Override
    public void updateArray(int columnIndex, Array x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateArray");
    }

    @Override
    public void updateArray(String columnLabel, Array x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateArray");
    }

    @Override
    public RowId getRowId(int columnIndex)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("getRowId");
    }

    @Override
    public RowId getRowId(String columnLabel)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("getRowId");
    }

    @Override
    public void updateRowId(int columnIndex, RowId x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateRowId");
    }

    @Override
    public void updateRowId(String columnLabel, RowId x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateRowId");
    }

    @Override
    public int getHoldability()
            throws SQLException
    {
        checkOpen();
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public abstract boolean isClosed()
            throws SQLException;

    @Override
    public void updateNString(int columnIndex, String nString)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateNString");
    }

    @Override
    public void updateNString(String columnLabel, String nString)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateNString");
    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateNClob");
    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateNClob");
    }

    @Override
    public NClob getNClob(int columnIndex)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("getNClob");
    }

    @Override
    public NClob getNClob(String columnLabel)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("getNClob");
    }

    @Override
    public SQLXML getSQLXML(int columnIndex)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("getSQLXML");
    }

    @Override
    public SQLXML getSQLXML(String columnLabel)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("getSQLXML");
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateSQLXML");
    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateSQLXML");
    }

    @Override
    public String getNString(int columnIndex)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("getNString");
    }

    @Override
    public String getNString(String columnLabel)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("getNString");
    }

    @Override
    public Reader getNCharacterStream(int columnIndex)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("getNCharacterStream");
    }

    @Override
    public Reader getNCharacterStream(String columnLabel)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("getNCharacterStream");
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateNCharacterStream");
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateNCharacterStream");
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateAsciiStream");
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateBinaryStream");
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateCharacterStream");
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateAsciiStream");
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateBinaryStream");
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateCharacterStream");
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateBlob");
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateBlob");
    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateClob");
    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateClob");
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateNClob");
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateNClob");
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateNCharacterStream");
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateNCharacterStream");
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateAsciiStream");
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateBinaryStream");
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateCharacterStream");
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateAsciiStream");
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateBinaryStream");
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateCharacterStream");
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateBlob");
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateBlob");
    }

    @Override
    public void updateClob(int columnIndex, Reader reader)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateClob");
    }

    @Override
    public void updateClob(String columnLabel, Reader reader)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateClob");
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateNClob");
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader)
            throws SQLException
    {
        throw new SQLFeatureNotSupportedException("updateNClob");
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type)
            throws SQLException
    {
        if (type == null) {
            throw new SQLException("type is null");
        }
        Object object = column(columnIndex);
        if (object == null) {
            return null;
        }

        return type.cast(object);
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type)
            throws SQLException
    {
        return getObject(columnIndex(columnLabel), type);
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
}
