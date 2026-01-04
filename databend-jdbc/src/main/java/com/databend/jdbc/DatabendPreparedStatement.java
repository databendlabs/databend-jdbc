package com.databend.jdbc;

import com.databend.client.StageAttachment;
import com.databend.client.data.DatabendRawType;
import com.databend.jdbc.parser.BatchInsertUtils;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.*;
import java.math.BigDecimal;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.*;
import java.time.*;
import java.time.format.DateTimeFormatterBuilder;
import java.util.*;
import java.util.function.Consumer;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

import static com.databend.jdbc.DatabendConstant.*;
import static com.databend.jdbc.ObjectCasts.*;
import static com.databend.jdbc.StatementUtil.replaceParameterMarksWithValues;
import static java.lang.String.format;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_TIME;
import static java.util.Objects.requireNonNull;

public class DatabendPreparedStatement extends DatabendStatement implements PreparedStatement {
    private static final Logger logger = Logger.getLogger(DatabendPreparedStatement.class.getPackage().getName());
    static final java.time.format.DateTimeFormatter DATE_FORMATTER = ISO_LOCAL_DATE;
    private final RawStatementWrapper rawStatement;
    static final java.time.format.DateTimeFormatter TIME_FORMATTER = java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss.SSS");
    static final java.time.format.DateTimeFormatter TIMESTAMP_FORMATTER = java.time.format.DateTimeFormatter
            .ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS")
            .withZone(ZoneOffset.UTC);
    private final DatabendParameterMetaData paramMetaData;
    private static final java.time.format.DateTimeFormatter LOCAL_DATE_TIME_FORMATTER = new DateTimeFormatterBuilder()
            .append(ISO_LOCAL_DATE)
            .appendLiteral(' ')
            .append(ISO_LOCAL_TIME)
            .toFormatter();
    private static final java.time.format.DateTimeFormatter OFFSET_TIME_FORMATTER = new DateTimeFormatterBuilder()
            .append(ISO_LOCAL_TIME)
            .appendOffset("+HH:mm", "+00:00")
            .toFormatter();
    private static final java.time.format.DateTimeFormatter OFFSET_DATE_TIME_FORMATTER = new DateTimeFormatterBuilder()
            .append(ISO_LOCAL_DATE)
            .appendLiteral(' ')
            .append(ISO_LOCAL_TIME)
            .appendOffset("+HH:mm", "+00:00")
            .toFormatter();
    private final List<String[]> batchValues;
    private final List<String[]> batchValuesCSV;
    private final BatchInsertUtils batchInsertUtils;

    private static final ObjectMapper objectMapper = new ObjectMapper();

    DatabendPreparedStatement(DatabendConnection connection, Consumer<DatabendStatement> onClose, String sql) throws SQLException {
        super(connection, onClose);
        this.batchValues = new ArrayList<>();
        this.batchValuesCSV = new ArrayList<>();
        this.batchInsertUtils = new BatchInsertUtils(sql);
        this.rawStatement = StatementUtil.parseToRawStatementWrapper(sql);
        if(this.rawStatement.getSubStatements().size() > 1) {
            throw new SQLException("Databend do not support multi statement for now");
        }
        Map<Integer, String> params = StatementUtil.extractColumnTypes(sql);
        List<DatabendColumnInfo> list = params.entrySet().stream().map(entry -> {
            String type = entry.getValue();
            DatabendRawType databendRawType = new DatabendRawType(type);
            return DatabendColumnInfo.of(entry.getKey().toString(), databendRawType);
        }).collect(Collectors.toList());
        this.paramMetaData = new DatabendParameterMetaData(Collections.unmodifiableList(list), new JdbcTypeMapping());
    }

    private static String formatBooleanLiteral(boolean x) {
        return Boolean.toString(x);
    }

    private static String formatByteLiteral(byte x) {
        return Byte.toString(x);
    }

    private static String formatShortLiteral(short x) {
        return Short.toString(x);
    }

    private static String formatIntLiteral(int x) {
        return Integer.toString(x);
    }

    private static String formatLongLiteral(long x) {
        return Long.toString(x);
    }

    private static String formatFloatLiteral(float x) {
        return Float.toString(x);
    }

    private static String formatDoubleLiteral(double x) {
        return Double.toString(x);
    }

    private static String formatBigDecimalLiteral(BigDecimal x) {
        return x.toString();
    }

    private static String formatBytesLiteral(byte[] x) {
        return new String(x, StandardCharsets.UTF_8);
    }

    static IllegalArgumentException invalidConversion(Object x, String toType) {
        return new IllegalArgumentException(
                format("Cannot convert instance of %s to %s", x.getClass().getName(), toType));
    }

    @Override
    public void close()
            throws SQLException {
        super.close();
    }

    private StageAttachment uploadBatches() throws SQLException {
        if (this.batchValuesCSV == null || this.batchValuesCSV.size() == 0) {
            return null;
        }
        File saved = batchInsertUtils.saveBatchToCSV(batchValuesCSV);
        try (FileInputStream fis = new FileInputStream(saved)) {
            Connection c = getConnection();
            String uuid = UUID.randomUUID().toString().replace("-", "");
            // format %Y/%m/%d/%H/%M/%S/fileName.csv
            String stagePrefix = String.format("%s/%s/%s/%s/%s/%s/%s/",
                    LocalDateTime.now().getYear(),
                    LocalDateTime.now().getMonthValue(),
                    LocalDateTime.now().getDayOfMonth(),
                    LocalDateTime.now().getHour(),
                    LocalDateTime.now().getMinute(),
                    LocalDateTime.now().getSecond(),
                    uuid);
            String fileName = saved.getName();
            // upload to stage
            c.unwrap(DatabendConnection.class).uploadStream(null, stagePrefix, fis, fileName, saved.length(), false);
            String stagePath = "@~/" + stagePrefix + fileName;
            return buildStateAttachment((DatabendConnection) c, stagePath);
        } catch (Exception e) {
            throw new SQLException(e);
        } finally {
            try {
                if (saved != null) {
                    saved.delete();
                }
            } catch (Exception e) {
                // ignore
            }
        }
    }

    // only for compat test
    static StageAttachment buildStateAttachment(DatabendConnection conn, String stagePath) {
        return buildStateAttachment((Connection) conn, stagePath);
    }

    /**
     * This method is used to build a StageAttachment object which represents a
     * stage in Databend.
     * A stage in Databend is a temporary storage area where data files are stored
     * before being loaded into the Databend database.
     *
     * @param conn The DatabendConnection object which contains the connection
     *                   details to the Databend database.
     * @param stagePath  The path of the stage in the Databend database.
     * @return A StageAttachment object which contains the details of the stage.
     */
    static StageAttachment buildStateAttachment(Connection conn, String stagePath) {
        DatabendConnection connection = (DatabendConnection) conn;

        Map<String, String> fileFormatOptions = new HashMap<>();
        if (!Objects.equals(connection.binaryFormat(), "")) {
            fileFormatOptions.put("binary_format", String.valueOf(connection.binaryFormat()));
        }
        Map<String, String> copyOptions = new HashMap<>();
        copyOptions.put("PURGE", String.valueOf(connection.copyPurge()));
        copyOptions.put("NULL_DISPLAY", String.valueOf(connection.nullDisplay()));
        StageAttachment attachment;
        if (fileFormatOptions.size() != 0) {
            attachment = new StageAttachment.Builder()
                    .setLocation(stagePath)
                    .setCopyOptions(copyOptions)
                    .setFileFormatOptions(fileFormatOptions)
                    .build();
        } else {
            attachment = new StageAttachment.Builder()
                    .setLocation(stagePath)
                    .setCopyOptions(copyOptions)
                    .build();
        }
        return attachment;
    }

    /**
     * delete stage file on stage attachment
     *
     * @return true if delete success or resource not found
     */
    boolean dropStageAttachment(StageAttachment attachment) {
        if (attachment == null) {
            return true;
        }
        String sql = String.format("REMOVE %s", attachment.getLocation());
        try {
            execute(sql);
            return true;
        } catch (SQLException e) {
            return e.getErrorCode() == 1003;
        }
    }

    int[] executeBatchByAttachment() throws SQLException {
        int[] batchUpdateCounts = new int[batchValues.size()];
        if (batchValues.isEmpty()) {
            return batchUpdateCounts;
        }
        StageAttachment attachment = uploadBatches();
        if (attachment == null) {
            return batchUpdateCounts;
        }
        try {
            logger.fine(String.format("use batch insert instead of normal insert, attachment: %s, sql: %s", attachment,
                    batchInsertUtils.getSql()));
            super.internalExecute(batchInsertUtils.getSql(), attachment);
            try (ResultSet r = getResultSet()) {
                while (r.next()) {
                }
            }
            Arrays.fill(batchUpdateCounts, 1);
            return batchUpdateCounts;
        } finally {
            dropStageAttachment(attachment);
            clearBatch();
        }
    }

    @Override
    public ResultSet executeQuery()
            throws SQLException {
        execute();
        return getResultSet();
    }


    @Override
    public int executeUpdate() throws SQLException {
        execute();
        return getUpdateCount();
    }

    @Override
    public boolean execute()
            throws SQLException {
        String sql = replaceParameterMarksWithValues(batchInsertUtils.getProvideParams(), this.rawStatement).get(0).getSql();
        return execute(sql);
    }

    @Override
    public int[] executeBatch() throws SQLException {
        if (isBatchInsert(batchInsertUtils.getSql())) {
            return executeBatchByAttachment();
        } else {
            int[] batchUpdateCounts = new int[batchValues.size()];
            for (int i = 0; i < batchValues.size(); i++) {
                String [] values = batchValues.get(i);
                Map<Integer, String> m = new HashMap<>();
                for (int j = 0; j< values.length; j++){
                    m.put(j + 1, values[j]);
                }
                String sql = replaceParameterMarksWithValues(m, this.rawStatement).get(0).getSql();
                this.execute(sql);
                batchUpdateCounts[i]= getUpdateCount();
            }
            return batchUpdateCounts;
        }
    }

    private static boolean isBatchInsert(String sql) {
        sql = sql.toLowerCase();
        Matcher matcher = INSERT_INTO_PATTERN.matcher(sql);
        return matcher.find() && !sql.contains(DATABEND_KEYWORDS_SELECT);
    }

    private void setValueStringNoQuote(int index, String value) {
        batchInsertUtils.setPlaceHolderValue(index, value, value);
    }

    private void setValueString(int index, String value) {
        batchInsertUtils.setPlaceHolderValue(index, String.format("'%s'", value), value);
    }

    private void setValueNull(int index) {
        setValue(index, "null", "\\N");
    }

    private void setValue(int index, String value, String csvValue) {
        batchInsertUtils.setPlaceHolderValue(index, value, csvValue);
    }

    @Override
    public void setNull(int i, int i1)
            throws SQLException {
        checkOpen();
        setValueNull(i);
    }

    @Override
    public void setBoolean(int i, boolean b)
            throws SQLException {
        checkOpen();
        setValueStringNoQuote(i, formatBooleanLiteral(b));
    }

    @Override
    public void setByte(int i, byte b)
            throws SQLException {
        checkOpen();
        setValueStringNoQuote(i, formatByteLiteral(b));
    }

    @Override
    public void setShort(int i, short i1)
            throws SQLException {
        checkOpen();
        setValueStringNoQuote(i, formatShortLiteral(i1));
    }

    @Override
    public void setInt(int i, int i1)
            throws SQLException {
        checkOpen();
        setValueStringNoQuote(i, formatIntLiteral(i1));
    }

    @Override
    public void setLong(int i, long l)
            throws SQLException {
        checkOpen();
        setValueStringNoQuote(i, formatLongLiteral(l));
    }

    @Override
    public void setFloat(int i, float v)
            throws SQLException {
        checkOpen();
        setValueStringNoQuote(i, formatFloatLiteral(v));
    }

    @Override
    public void setDouble(int i, double v)
            throws SQLException {
        checkOpen();
        setValueStringNoQuote(i, formatDoubleLiteral(v));
    }

    @Override
    public void setBigDecimal(int i, BigDecimal v)
            throws SQLException {
        checkOpen();
        if (v == null) {
            setValueNull(i);
        } else {
            setValueStringNoQuote(i, formatBigDecimalLiteral(v));
        }
    }

    @Override
    public void setString(int i, String s)
            throws SQLException {
        checkOpen();
        String quoted = s;
        if (s == null) {
            setValueNull(i);
            return;
        }
        if (s.contains("'")) {
            quoted = s.replace("'", "\\'");
        }
        quoted = String.format("'%s'", quoted);

        setValue(i, quoted, s);
    }

    @Override
    public void setBytes(int i, byte[] v)
            throws SQLException {
        checkOpen();
        setValueStringNoQuote(i, formatBytesLiteral(v));
    }

    @Override
    public void setDate(int i, Date date)
            throws SQLException {
        checkOpen();
        if (date == null) {
            setValueNull(i);
        } else {
            setValueString(i, date.toString());
        }
    }

    @Override
    public void setTime(int i, Time v)
            throws SQLException {
        checkOpen();
        if (v == null) {
            setValueNull(i);
        } else {
            setValueString(i, v.toString());
        }
    }

    @Override
    public void setTimestamp(int i, Timestamp v)
            throws SQLException {
        checkOpen();
        if (v == null) {
            setValueNull(i);
        } else {
            setValueString(i, v.toInstant().toString());
        }
    }

    @Override
    public void setAsciiStream(int i, InputStream inputStream, int i1)
            throws SQLException {
        throw new SQLFeatureNotSupportedException("setAsciiStream not supported");
    }

    @Override
    public void setUnicodeStream(int i, InputStream inputStream, int i1)
            throws SQLException {
        throw new SQLFeatureNotSupportedException("setUnicodeStream not supported");
    }

    @Override
    public void setBinaryStream(int i, InputStream inputStream, int i1)
            throws SQLException {
        throw new SQLFeatureNotSupportedException("setBinaryStream not supported");
    }

    @Override
    public void clearParameters()
            throws SQLException {
        checkOpen();
        batchInsertUtils.clean();
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType)
            throws SQLException {
        checkOpen();
        if (x == null) {
            setValueNull(parameterIndex);
            return;
        }
        switch (targetSqlType) {
            case Types.BOOLEAN:
            case Types.BIT:
                setBoolean(parameterIndex, castToBoolean(x, targetSqlType));
                return;
            case Types.TINYINT:
                setByte(parameterIndex, castToByte(x, targetSqlType));
                return;
            case Types.SMALLINT:
                setShort(parameterIndex, castToShort(x, targetSqlType));
                return;
            case Types.INTEGER:
                setInt(parameterIndex, castToInt(x, targetSqlType));
                return;
            case Types.BIGINT:
                setLong(parameterIndex, castToLong(x, targetSqlType));
                return;
            case Types.FLOAT:
            case Types.REAL:
                setFloat(parameterIndex, castToFloat(x, targetSqlType));
                return;
            case Types.DOUBLE:
                setDouble(parameterIndex, castToDouble(x, targetSqlType));
                return;
            case Types.DECIMAL:
            case Types.NUMERIC:
                setBigDecimal(parameterIndex, castToBigDecimal(x, targetSqlType));
                return;
            case Types.CHAR:
            case Types.NCHAR:
            case Types.VARCHAR:
            case Types.NVARCHAR:
            case Types.LONGVARCHAR:
            case Types.LONGNVARCHAR:
                if (x instanceof Duration) {
                    setDurationLiteral(parameterIndex, (Duration) x);
                } else {
                    setString(parameterIndex, x.toString());
                }
                return;
            case Types.BINARY:
                InputStream blobInputStream = new ByteArrayInputStream(x.toString().getBytes());
                setBinaryStream(parameterIndex, blobInputStream);
                return;
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                setBytes(parameterIndex, castToBinary(x, targetSqlType));
                return;
            case Types.DATE:
                setString(parameterIndex, toDateLiteral(x));
                return;
            case Types.TIME:
                setString(parameterIndex, toTimeLiteral(x));
                return;
            case Types.TIME_WITH_TIMEZONE:
                setString(parameterIndex, toTimeWithTimeZoneLiteral(x));
                return;
            case Types.TIMESTAMP:
            case Types.TIMESTAMP_WITH_TIMEZONE:
                setString(parameterIndex, toTimestampLiteral(x));
                return;
            case Types.OTHER:
            case Types.JAVA_OBJECT:
                if (x instanceof Duration) {
                    setDurationLiteral(parameterIndex, (Duration) x);
                    return;
                }
                break;
        }
        throw new SQLException("Unsupported target SQL type: " + targetSqlType);
    }

    @Override
    public void setObject(int parameterIndex, Object x)
            throws SQLException {
        checkOpen();
        if (x == null) {
            setValueNull(parameterIndex);
        } else if (x instanceof Boolean) {
            setBoolean(parameterIndex, (Boolean) x);
        } else if (x instanceof Byte) {
            setByte(parameterIndex, (Byte) x);
        } else if (x instanceof Short) {
            setShort(parameterIndex, (Short) x);
        } else if (x instanceof Integer) {
            setInt(parameterIndex, (Integer) x);
        } else if (x instanceof Long) {
            setLong(parameterIndex, (Long) x);
        } else if (x instanceof Float) {
            setFloat(parameterIndex, (Float) x);
        } else if (x instanceof Double) {
            setDouble(parameterIndex, (Double) x);
        } else if (x instanceof BigDecimal) {
            setBigDecimal(parameterIndex, (BigDecimal) x);
        } else if (x instanceof String) {
            setString(parameterIndex, (String) x);
        } else if (x instanceof byte[]) {
            setBytes(parameterIndex, (byte[]) x);
        } else if (x instanceof Date) {
            setDate(parameterIndex, (Date) x);
        } else if (x instanceof LocalDate) {
            setString(parameterIndex, x.toString());
        } else if (x instanceof Time) {
            setTime(parameterIndex, (Time) x);
        } else if (x instanceof OffsetTime) {
            setString(parameterIndex, toTimeWithTimeZoneLiteral(x));
        } else if (x instanceof Instant) {
            setValueString(parameterIndex, x.toString());
        } else if (x instanceof LocalDateTime) {
            setValueString(parameterIndex, x.toString());
        } else if (x instanceof Timestamp) {
            setTimestamp(parameterIndex, (Timestamp) x);
        } else if (x instanceof OffsetDateTime) {
            setString(parameterIndex, toTimestampLiteral(x));
        } else if (x instanceof ZonedDateTime) {
            setString(parameterIndex, toTimestampLiteral(x));
        } else if (x instanceof Duration) {
            setDurationLiteral(parameterIndex, (Duration) x);
        } else if (x instanceof Map) {
            setString(parameterIndex, convertToJsonString((Map<?, ?>) x));
        } else if (x instanceof Array) {
            setString(parameterIndex, convertArrayToString((Array) x));
        } else if (x instanceof ArrayList) {
            setString(parameterIndex, convertArrayListToString((ArrayList<?>) x));
        } else {
            throw new SQLException("Unsupported object type: " + x.getClass().getName());
        }
    }

    public static String convertToJsonString(Map<?, ?> map) {
        try {
            return objectMapper.writeValueAsString(map);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to convert map to JSON string", e);
        }
    }

    public static String convertArrayToString(Array array) {
        return array.toString();
    }

    public static String convertArrayListToString(ArrayList<?> arrayList) {
        StringBuilder builder = new StringBuilder();
        builder.append("[");
        for (int i = 0; i < arrayList.size(); i++) {
            builder.append(arrayList.get(i));
            if (i < arrayList.size() - 1) {
                builder.append(", ");
            }
        }
        builder.append("]");

        return builder.toString();
    }

    private void setDurationLiteral(int parameterIndex, Duration duration) throws SQLException {
        try {
            setString(parameterIndex, Interval.encode(duration));
        } catch (IllegalArgumentException ex) {
            throw new SQLException("Failed to encode Duration for interval parameter", ex);
        }
    }

    @Override
    public void addBatch()
            throws SQLException {
        checkOpen();

        String[] val = batchInsertUtils.getValues();
        batchValues.add(val);

        val = batchInsertUtils.getValuesCSV();
        batchValuesCSV.add(val);

        batchInsertUtils.clean();
    }

    @Override
    public void clearBatch() throws SQLException {
        checkOpen();
        batchValues.clear();
        batchValuesCSV.clear();
        batchInsertUtils.clean();
    }

    @Override
    public void setCharacterStream(int i, Reader reader, int i1)
            throws SQLException {
        throw new SQLFeatureNotSupportedException("PreparedStatement", "setCharacterStream");
    }

    @Override
    public void setRef(int i, Ref ref)
            throws SQLException {
        throw new SQLFeatureNotSupportedException("PreparedStatement", "setRef");
    }


    @Override
    public void setClob(int i, Clob x)
            throws SQLException {
        throw new SQLFeatureNotSupportedException("PreparedStatement", "setClob");
    }

    @Override
    public void setArray(int i, Array array)
            throws SQLException {
        throw new SQLFeatureNotSupportedException("PreparedStatement", "setArray");
    }

    @Override
    public ResultSetMetaData getMetaData()
            throws SQLException {
        return null;
    }

    @Override
    public void setDate(int i, Date date, Calendar calendar)
            throws SQLException {
        throw new SQLFeatureNotSupportedException("PreparedStatement", "setDate(int, Date, Calendar)");
    }

    @Override
    public void setTime(int i, Time time, Calendar calendar)
            throws SQLException {
        throw new SQLFeatureNotSupportedException("PreparedStatement", "setTime(int, Time, Calendar)");
    }

    @Override
    public void setTimestamp(int i, Timestamp timestamp, Calendar calendar)
            throws SQLException {
        String s = OFFSET_DATE_TIME_FORMATTER.format(timestamp.toInstant().atZone(calendar.getTimeZone().toZoneId()));
        setValueString(i, s);
    }

    @Override
    public void setNull(int i, int i1, String s)
            throws SQLException {
        setValueNull(i);
    }

    @Override
    public void setURL(int i, URL url)
            throws SQLException {
        throw new SQLFeatureNotSupportedException("PreparedStatement", "setURL");
    }

    // If you want to use ps.getParameterMetaData().* methods, you need to use a
    // valid sql such as
    // insert into table_name (col1 type1, col2 typ2, col3 type3) values (?, ?, ?)
    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        return paramMetaData;
    }

    @Override
    public void setRowId(int i, RowId rowId)
            throws SQLException {
        throw new SQLFeatureNotSupportedException("PreparedStatement", "setRowId");
    }

    @Override
    public void setNString(int i, String s)
            throws SQLException {
        throw new SQLFeatureNotSupportedException("PreparedStatement", "setNString");
    }

    @Override
    public void setNCharacterStream(int i, Reader reader, long l)
            throws SQLException {
        throw new SQLFeatureNotSupportedException("PreparedStatement", "setNCharacterStream");
    }

    @Override
    public void setNClob(int i, NClob nClob)
            throws SQLException {
        throw new SQLFeatureNotSupportedException("PreparedStatement", "setNClob");
    }

    @Override
    public void setClob(int i, Reader reader, long l)
            throws SQLException {
        throw new SQLFeatureNotSupportedException("PreparedStatement", "setClob(int, Reader, long)");
    }



    @Override
    public void setNClob(int i, Reader reader, long l)
            throws SQLException {
        throw new SQLFeatureNotSupportedException("PreparedStatement", "setNClob");
    }

    @Override
    public void setSQLXML(int i, SQLXML sqlxml)
            throws SQLException {
        throw new SQLFeatureNotSupportedException("PreparedStatement", "setSQLXML");
    }

    @Override
    public void setObject(int i, Object o, int i1, int i2)
            throws SQLException {
        throw new SQLFeatureNotSupportedException("PreparedStatement", "setObject(int, Object, int, int)");
    }

    @Override
    public void setAsciiStream(int i, InputStream inputStream, long l)
            throws SQLException {
        throw new SQLFeatureNotSupportedException("PreparedStatement", "setAsciiStream");
    }

    @Override
    public void setBinaryStream(int i, InputStream inputStream, long l)
            throws SQLException {
        throw new SQLFeatureNotSupportedException("PreparedStatement", "setBinaryStream");
    }

    @Override
    public void setCharacterStream(int i, Reader reader, long l)
            throws SQLException {
        throw new SQLFeatureNotSupportedException("PreparedStatement", "setCharacterStream");
    }

    @Override
    public void setAsciiStream(int i, InputStream inputStream)
            throws SQLException {
        throw new SQLFeatureNotSupportedException("PreparedStatement", "setAsciiStream");
    }

    @Override
    public void setBinaryStream(int i, InputStream inputStream)
            throws SQLException {
        checkOpen();
        if (inputStream == null) {
            setValueNull(i);
            return;
        }
        try {
            ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            int nRead;
            byte[] data = new byte[1024];
            while ((nRead = inputStream.read(data, 0, data.length)) != -1) {
                buffer.write(data, 0, nRead);
            }
            buffer.flush();
            byte[] bytes = buffer.toByteArray();
            if (BASE64_STR.equalsIgnoreCase(connection().binaryFormat())) {
                String base64String = bytesToBase64(bytes);
                setValueStringNoQuote(i, base64String);
            } else {
                String hexString = bytesToHex(bytes);
                setValueStringNoQuote(i, hexString);
            }
        } catch (IOException e) {
            throw new SQLException("Error reading InputStream", e);
        }
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    private static String bytesToBase64(byte[] bytes) {
        return Base64.getEncoder().encodeToString(bytes);
    }

    @Override
    public void setCharacterStream(int i, Reader reader)
            throws SQLException {
        throw new SQLFeatureNotSupportedException("PreparedStatement", "setCharacterStream");
    }

    @Override
    public void setNCharacterStream(int i, Reader reader)
            throws SQLException {
        throw new SQLFeatureNotSupportedException("PreparedStatement", "setNCharacterStream");
    }

    @Override
    public void setClob(int i, Reader reader)
            throws SQLException {
        throw new SQLFeatureNotSupportedException("PreparedStatement", "setClob(int, Reader)");
    }

    public void setBlob(int i, Blob x)
            throws SQLException {
        // never get null
        setBinaryStream(i, x.getBinaryStream());
    }

    @Override
    public void setBlob(int i, InputStream inputStream)
            throws SQLException {
        // never get null
        setBinaryStream(i, inputStream);
    }

    @Override
    public void setBlob(int i, InputStream inputStream, long l)
            throws SQLException {
        throw new SQLFeatureNotSupportedException("PreparedStatement", "setBlob(int, InputStream, long)");
    }

    @Override
    public void setNClob(int i, Reader reader)
            throws SQLException {
        throw new SQLFeatureNotSupportedException("PreparedStatement", "setNClob");
    }

    private String toDateLiteral(Object value) throws IllegalArgumentException {
        requireNonNull(value, "value is null");
        if (value instanceof java.util.Date) {
            Instant instant = ((java.util.Date) value).toInstant();
            LocalDate localDate = instant.atZone(ZoneId.systemDefault()).toLocalDate();
            return DATE_FORMATTER.format(localDate);
        }
        if (value instanceof LocalDate) {
            return ISO_LOCAL_DATE.format(((LocalDate) value));
        }
        if (value instanceof LocalDateTime) {
            return ISO_LOCAL_DATE.format(((LocalDateTime) value));
        }
        if (value instanceof String) {
            // TODO validate proper format
            return (String) value;
        }
        throw invalidConversion(value, "date");
    }

    private String toTimeLiteral(Object value)
            throws IllegalArgumentException {
        if (value instanceof java.util.Date) {
            Instant instant = ((java.util.Date) value).toInstant();
            LocalTime localTime = instant.atZone(ZoneId.systemDefault()).toLocalTime();
            return TIME_FORMATTER.format(localTime);
        }
        if (value instanceof LocalTime) {
            return ISO_LOCAL_TIME.format((LocalTime) value);
        }
        if (value instanceof LocalDateTime) {
            return ISO_LOCAL_TIME.format((LocalDateTime) value);
        }
        if (value instanceof String) {
            // TODO validate proper format
            return (String) value;
        }
        throw invalidConversion(value, "time");
    }

    private String toTimestampLiteral(Object value)
            throws IllegalArgumentException {
        if (value instanceof java.util.Date) {
            // include java.sql.Timestamp
            return TIMESTAMP_FORMATTER.format(((java.util.Date) value).toInstant()) + "Z";
        }
        if (value instanceof OffsetDateTime) {
            return OFFSET_DATE_TIME_FORMATTER.format((OffsetDateTime) value);
        }
        if (value instanceof ZonedDateTime) {
            return OFFSET_DATE_TIME_FORMATTER.format(((ZonedDateTime) value).toOffsetDateTime());
        }
        if (value instanceof LocalDateTime) {
            return LOCAL_DATE_TIME_FORMATTER.format(((LocalDateTime) value));
        }
        if (value instanceof String) {
            // TODO validate proper format
            return (String) value;
        }
        throw invalidConversion(value, "timestamp");
    }

    private String toTimeWithTimeZoneLiteral(Object value)
            throws SQLException {
        if (value instanceof OffsetTime) {
            return OFFSET_TIME_FORMATTER.format((OffsetTime) value);
        }
        if (value instanceof String) {
            // TODO validate proper format
            return (String) value;
        }
        throw invalidConversion(value, "time with time zone");
    }
}
