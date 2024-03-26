package com.databend.jdbc;

import com.databend.client.*;
import com.databend.jdbc.annotation.NotImplemented;
import com.databend.jdbc.cloud.DatabendCopyParams;
import com.databend.jdbc.cloud.DatabendPresignClient;
import com.databend.jdbc.cloud.DatabendPresignClientV1;
import com.databend.jdbc.exception.DatabendFailedToPingException;
import com.fasterxml.jackson.core.JsonProcessingException;
import okhttp3.Headers;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPOutputStream;

import static com.databend.client.ClientSettings.*;
import static com.databend.client.DatabendClientV1.*;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Collections.newSetFromMap;
import static java.util.Objects.requireNonNull;

public class DatabendConnection implements Connection, FileTransferAPI {
    private static final Logger logger = Logger.getLogger(DatabendConnection.class.getPackage().getName());
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicBoolean autoCommit = new AtomicBoolean(true);
    private final URI httpUri;
    private final AtomicReference<String> schema = new AtomicReference<>();
    private final OkHttpClient httpClient;
    private final Set<DatabendStatement> statements = newSetFromMap(new ConcurrentHashMap<>());
    private final DatabendDriverUri driverUri;
    private AtomicReference<DatabendSession> session = new AtomicReference<>();

    DatabendConnection(DatabendDriverUri uri, OkHttpClient httpClient) throws SQLException {
        requireNonNull(uri, "uri is null");
        this.httpUri = uri.getUri();
        this.setSchema(uri.getDatabase());
        this.httpClient = httpClient;
        this.driverUri = uri;
        DatabendSession session = new DatabendSession.Builder().setHost(this.getURI()).setDatabase(this.getSchema()).build();
        this.setSession(session);
    }

    private static void checkResultSet(int resultSetType, int resultSetConcurrency)
            throws SQLFeatureNotSupportedException {
        if (resultSetType != ResultSet.TYPE_FORWARD_ONLY) {
            throw new SQLFeatureNotSupportedException("Result set type must be TYPE_FORWARD_ONLY");
        }
        if (resultSetConcurrency != ResultSet.CONCUR_READ_ONLY) {
            throw new SQLFeatureNotSupportedException("Result set concurrency must be CONCUR_READ_ONLY");
        }
    }

    // Databend DOES NOT support transaction now
    private static void checkHoldability(int resultSetHoldability)
            throws SQLFeatureNotSupportedException {
        if (resultSetHoldability != ResultSet.HOLD_CURSORS_OVER_COMMIT) {
            throw new SQLFeatureNotSupportedException("Result set holdability must be HOLD_CURSORS_OVER_COMMIT");
        }
    }

    public static String getCopyIntoSql(String database, DatabendCopyParams params) {
        StringBuilder sb = new StringBuilder();
        sb.append("COPY INTO ");
        if (database != null) {
            sb.append(database).append(".");
        }
        sb.append(params.getDatabaseTableName()).append(" ");
        sb.append("FROM ");
        sb.append(params.getDatabendStage().toString());
        sb.append(" ");
        sb.append(params.toString());
        return sb.toString();
    }

    public DatabendSession getSession() {
        return this.session.get();
    }

    public void setSession(DatabendSession session) {
        if (session == null) {
            return;
        }
        this.session.set(session);
    }

    public OkHttpClient getHttpClient() {
        return httpClient;
    }

    @Override
    public Statement createStatement()
            throws SQLException {
        return doCreateStatement();
    }

    private DatabendStatement doCreateStatement() throws SQLException {
        checkOpen();
        DatabendStatement statement = new DatabendStatement(this, this::unregisterStatement);
        registerStatement(statement);
        return statement;
    }

    private void registerStatement(DatabendStatement statement) {
        checkState(statements.add(statement), "Statement is already registered");
    }

    private void unregisterStatement(DatabendStatement statement) {
        checkState(statements.remove(statement), "Statement is not registered");
    }

    @Override
    public PreparedStatement prepareStatement(String s)
            throws SQLException {

        return this.prepareStatement(s, 0, 0);
    }

    @Override
    public CallableStatement prepareCall(String s)
            throws SQLException {
        throw new SQLFeatureNotSupportedException("prepareCall");
    }

    @Override
    public String nativeSQL(String sql)
            throws SQLException {
        checkOpen();
        return sql;
    }

    private void checkOpen()
            throws SQLException {
        if (isClosed()) {
            throw new SQLException("Connection is closed");
        }
    }

    @Override
    public void commit()
            throws SQLException {
        checkOpen();
        try {
            this.startQuery("commit");
        } catch (SQLException e) {
            throw new SQLException("Failed to commit", e);
        }
        return;
    }

    @Override
    public boolean getAutoCommit()
            throws SQLException {
        checkOpen();
        return autoCommit.get();
    }

    @Override
    public void setAutoCommit(boolean b)
            throws SQLException {
        this.session.get().setAutoCommit(b);
        autoCommit.set(b);
    }

    @Override
    public void rollback()
            throws SQLException {
        checkOpen();
        try {
            this.startQuery("rollback");
        } catch (SQLException e) {
            throw new SQLException("Failed to rollback", e);
        }
        return;
    }

    @Override
    public void close()
            throws SQLException {
        for (Statement stmt : statements) {
            stmt.close();
        }
    }

    @Override
    public boolean isClosed()
            throws SQLException {
        return closed.get();
    }

    @Override
    public DatabaseMetaData getMetaData()
            throws SQLException {
        return new DatabendDatabaseMetaData(this);
    }

    @Override
    public boolean isReadOnly()
            throws SQLException {
        return false;
    }

    @Override
    public void setReadOnly(boolean b)
            throws SQLException {

    }

    @Override
    public String getCatalog()
            throws SQLException {
        return null;
    }

    @Override
    public void setCatalog(String s)
            throws SQLException {

    }

    @Override
    public int getTransactionIsolation()
            throws SQLException {
        return 0;
    }

    @Override
    public void setTransactionIsolation(int i)
            throws SQLException {

    }

    @Override
    public SQLWarning getWarnings()
            throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings()
            throws SQLException {

    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency)
            throws SQLException {
        checkResultSet(resultSetType, resultSetConcurrency);
        return createStatement();
    }

    @Override
    public PreparedStatement prepareStatement(String s, int i, int i1)
            throws SQLException {
        DatabendPreparedStatement statement = new DatabendPreparedStatement(this, this::unregisterStatement, "test", s);
        registerStatement(statement);
        return statement;
    }

    @Override
    public CallableStatement prepareCall(String s, int i, int i1)
            throws SQLException {
        throw new SQLFeatureNotSupportedException("prepareCall");
    }

    @Override
    public Map<String, Class<?>> getTypeMap()
            throws SQLException {
        throw new SQLFeatureNotSupportedException("getTypeMap");
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map)
            throws SQLException {
        throw new SQLFeatureNotSupportedException("setTypeMap");
    }

    public int getHoldability() throws SQLException {
        return 0;
    }

    @Override
    @NotImplemented
    public void setHoldability(int holdability) throws SQLException {
        // No support for transaction
    }

    @Override
    public Savepoint setSavepoint()
            throws SQLException {
        throw new SQLFeatureNotSupportedException("setSavepoint");
    }

    @Override
    public Savepoint setSavepoint(String s)
            throws SQLException {
        throw new SQLFeatureNotSupportedException("setSavepoint");
    }

    @Override
    public void rollback(Savepoint savepoint)
            throws SQLException {
        throw new SQLFeatureNotSupportedException("rollback");

    }

    @Override
    public void releaseSavepoint(Savepoint savepoint)
            throws SQLException {
        throw new SQLFeatureNotSupportedException("releaseSavepoint");

    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
//        checkHoldability(resultSetHoldability);
        return createStatement(resultSetType, resultSetConcurrency);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
//        checkHoldability(resultSetHoldability);
        return prepareStatement(sql, resultSetType, resultSetConcurrency);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        return prepareCall(sql, resultSetType, resultSetConcurrency);
    }

    @Override
    public PreparedStatement prepareStatement(String s, int autoGeneratedKeys)
            throws SQLException {
        return prepareStatement(s);
    }

    @Override
    public PreparedStatement prepareStatement(String s, int[] ints)
            throws SQLException {
        throw new SQLFeatureNotSupportedException("prepareStatement");
    }

    @Override
    public PreparedStatement prepareStatement(String s, String[] strings)
            throws SQLException {
        throw new SQLFeatureNotSupportedException("prepareStatement");
    }

    @Override
    public Clob createClob()
            throws SQLException {
        throw new SQLFeatureNotSupportedException("createClob");
    }

    @Override
    public Blob createBlob()
            throws SQLException {
        throw new SQLFeatureNotSupportedException("createBlob");
    }

    @Override
    public NClob createNClob()
            throws SQLException {
        throw new SQLFeatureNotSupportedException("createNClob");
    }

    @Override
    public SQLXML createSQLXML()
            throws SQLException {
        throw new SQLFeatureNotSupportedException("createSQLXML");
    }

    @Override
    public boolean isValid(int i)
            throws SQLException {
        return !isClosed();
    }

    @Override
    public void setClientInfo(String s, String s1)
            throws SQLClientInfoException {

    }

    @Override
    public String getClientInfo(String s)
            throws SQLException {
        return null;
    }

    @Override
    public Properties getClientInfo()
            throws SQLException {
        return null;
    }

    @Override
    public void setClientInfo(Properties properties)
            throws SQLClientInfoException {

    }

    @Override
    public Array createArrayOf(String s, Object[] objects)
            throws SQLException {
        throw new SQLFeatureNotSupportedException("createArrayOf");
    }

    @Override
    public Struct createStruct(String s, Object[] objects)
            throws SQLException {
        throw new SQLFeatureNotSupportedException("createStruct");
    }

    @Override
    public String getSchema()
            throws SQLException {
        checkOpen();
        return schema.get();
    }

    @Override
    public void setSchema(String schema)
            throws SQLException {
        checkOpen();
        this.schema.set(schema);
    }

    @Override
    public void abort(Executor executor)
            throws SQLException {
        close();
    }

    @Override
    public void setNetworkTimeout(Executor executor, int i)
            throws SQLException {

    }

    @Override
    public int getNetworkTimeout()
            throws SQLException {
        return 0;
    }

    @Override
    public <T> T unwrap(Class<T> aClass)
            throws SQLException {
        if (isWrapperFor(aClass)) {
            return (T) this;
        }
        throw new SQLException("No wrapper for " + aClass);
    }

    @Override
    public boolean isWrapperFor(Class<?> aClass)
            throws SQLException {
        return aClass.isInstance(this);
    }

    public boolean presignedUrlDisabled() {
        return this.driverUri.presignedUrlDisabled();
    }

    public boolean copyPurge() {
        return this.driverUri.copyPurge();
    }

    public String warehouse() {
        return this.driverUri.getWarehouse();
    }

    public Boolean strNullAsNull() {
        return this.driverUri.getStrNullAsNull();
    }

    public String tenant() {
        return this.driverUri.getTenant();
    }

    public String nullDisplay() {
        return this.driverUri.nullDisplay();
    }

    public String binaryFormat() {
        return this.driverUri.binaryFormat();
    }

    public PaginationOptions getPaginationOptions() {
        PaginationOptions.Builder builder = PaginationOptions.builder();
        builder.setWaitTimeSecs(this.driverUri.getWaitTimeSecs());
        builder.setMaxRowsInBuffer(this.driverUri.getMaxRowsInBuffer());
        builder.setMaxRowsPerPage(this.driverUri.getMaxRowsPerPage());
        return builder.build();
    }

    public URI getURI() {
        return this.httpUri;
    }

    private String buildUrlWithQueryRequest(ClientSettings settings, String querySql) {
        QueryRequest req = QueryRequest.builder()
                .setSession(settings.getSession())
                .setStageAttachment(settings.getStageAttachment())
                .setPaginationOptions(settings.getPaginationOptions())
                .setSql(querySql)
                .build();
        String reqString = req.toString();
        if (reqString == null || reqString.isEmpty()) {
            throw new IllegalArgumentException("Invalid request: " + req);
        }
        return reqString;
    }

    public void PingDatabendClientV1() throws SQLException {
        try {
            this.startQuery("select 1");
        } catch (SQLException e) {
            throw new SQLException("Failed to commit", e);
        }
    }


    DatabendClient startQuery(String sql) throws SQLException {
        return new DatabendClientV1(httpClient, sql, makeClientSettings());
    }

    DatabendClient startQuery(String sql, StageAttachment attach) throws SQLException {
        PaginationOptions options = getPaginationOptions();
        Map<String, String> additionalHeaders = setAdditionalHeaders();
        ClientSettings s = new ClientSettings.Builder().
                setSession(this.session.get()).
                setHost(this.getURI().toString()).
                setQueryTimeoutSecs(this.driverUri.getQueryTimeout()).
                setConnectionTimeout(this.driverUri.getConnectionTimeout()).
                setSocketTimeout(this.driverUri.getSocketTimeout()).
                setPaginationOptions(options).
                setAdditionalHeaders(additionalHeaders).
                setStageAttachment(attach).
                build();
        return new DatabendClientV1(httpClient, sql, s);
    }

    private ClientSettings makeClientSettings() {
        PaginationOptions options = getPaginationOptions();
        Map<String, String> additionalHeaders = setAdditionalHeaders();
        ClientSettings s = new ClientSettings.Builder().
                setSession(this.session.get()).
                setHost(this.getURI().toString()).
                setQueryTimeoutSecs(this.driverUri.getQueryTimeout()).
                setConnectionTimeout(this.driverUri.getConnectionTimeout()).
                setSocketTimeout(this.driverUri.getSocketTimeout()).
                setPaginationOptions(options).
                setAdditionalHeaders(additionalHeaders).
                build();
        return s;
    }

    private Map<String, String> setAdditionalHeaders() {
        Map<String, String> additionalHeaders = new HashMap<>();
        additionalHeaders.put(X_Databend_Query_ID, UUID.randomUUID().toString());
        if (!this.driverUri.getWarehouse().isEmpty()) {
            additionalHeaders.put(DatabendWarehouseHeader, this.driverUri.getWarehouse());
        }
        if (!this.driverUri.getTenant().isEmpty()) {
            additionalHeaders.put(DatabendTenantHeader, this.driverUri.getTenant());
        }
        return additionalHeaders;
    }


    /**
     * Method to put data from a stream at a stage location. The data will be uploaded as one file. No
     * splitting is done in this method.
     *
     * <p>Stream size must match the total size of data in the input stream unless compressData
     * parameter is set to true.
     *
     * <p>caller is responsible for passing the correct size for the data in the stream and releasing
     * the inputStream after the method is called.
     *
     * <p>Note this method is deprecated since streamSize is not required now. Keep the function
     * signature for backward compatibility
     *
     * @param stageName stage name: e.g. ~ or table name or stage name
     * @param destPrefix path prefix under which the data should be uploaded on the stage
     * @param inputStream input stream from which the data will be uploaded
     * @param destFileName destination file name to use
     * @param fileSize data size in the stream
     * @throws SQLException failed to put data from a stream at stage
     */
    @Override
    public void uploadStream(String stageName, String destPrefix, InputStream inputStream, String destFileName, long fileSize, boolean compressData)
            throws SQLException {
        /*
         remove / in the end of stage name
         remove / in the beginning of destPrefix and end of destPrefix
         */
        String s;
        if (stageName == null) {
            s = "~";
        } else {
            s = stageName.replaceAll("/$", "");
        }
        String p = destPrefix.replaceAll("^/", "").replaceAll("/$", "");
        String dest = p + "/" + destFileName;
        try {
            InputStream dataStream = inputStream;
            if (compressData) {
                // Wrap the input stream with a GZIPOutputStream for compression
                ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                try (GZIPOutputStream gzipOutputStream = new GZIPOutputStream(byteArrayOutputStream)) {
                    byte[] buffer = new byte[1024];
                    int len;
                    while ((len = inputStream.read(buffer)) != -1) {
                        gzipOutputStream.write(buffer, 0, len);
                    }
                }
                dataStream = new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
                fileSize = byteArrayOutputStream.size();  // Update the file size to the compressed size
            }
            if (this.driverUri.presignedUrlDisabled()) {
                DatabendPresignClient cli = new DatabendPresignClientV1(httpClient, this.httpUri.toString());
                cli.presignUpload(null, dataStream, s, p + "/", destFileName, fileSize, true);
            } else {
                logger.log(Level.FINE, "presign to @" + s + "/" + dest);
                PresignContext ctx = PresignContext.getPresignContext(this, PresignContext.PresignMethod.UPLOAD, s, dest);
                Headers h = ctx.getHeaders();
                String presignUrl = ctx.getUrl();
                DatabendPresignClient cli = new DatabendPresignClientV1(new OkHttpClient(), this.httpUri.toString());
                cli.presignUpload(null, dataStream, h, presignUrl, fileSize, true);
            }
        } catch (JsonProcessingException e) {
            System.out.println(e.getMessage());
            // For datax batch insert test, do not throw exception
            throw new SQLException(e);
        } catch (IOException e) {
            logger.warning("failed to upload input stream" + e.getMessage());
        }
    }

    @Override
    public InputStream downloadStream(String stageName, String sourceFileName, boolean decompress)
            throws SQLException {
        String s = stageName.replaceAll("/$", "");
        DatabendPresignClient cli = new DatabendPresignClientV1(httpClient, this.httpUri.toString());
        try {
            PresignContext ctx = PresignContext.getPresignContext(this, PresignContext.PresignMethod.DOWNLOAD, s, sourceFileName);
            Headers h = ctx.getHeaders();
            String presignUrl = ctx.getUrl();
            return cli.presignDownloadStream(h, presignUrl);
        } catch (JsonProcessingException e) {
            throw new SQLException(e);
        }
    }

    @Override
    public void copyIntoTable(String database, String tableName, DatabendCopyParams params)
            throws SQLException {
        DatabendCopyParams p = params == null ? DatabendCopyParams.builder().build() : params;
        requireNonNull(p.getDatabaseTableName(), "tableName is null");
        requireNonNull(p.getDatabendStage(), "stage is null");
        String sql = getCopyIntoSql(database, p);
        System.out.println(sql);
        Statement statement = this.createStatement();
        statement.execute(sql);
        ResultSet rs = statement.getResultSet();
        while (rs.next()) {
        }
    }
}
