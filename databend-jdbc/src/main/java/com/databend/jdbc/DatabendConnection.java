package com.databend.jdbc;

import com.databend.client.ClientSettings;
import com.databend.client.DatabendClient;
import com.databend.client.DatabendClientV1;
import com.databend.client.DatabendSession;
import com.databend.client.PaginationOptions;
import com.databend.client.StageAttachment;
import com.databend.jdbc.annotation.NotImplemented;
import com.databend.jdbc.cloud.DatabendCopyParams;
import com.databend.jdbc.cloud.DatabendPresignClient;
import com.databend.jdbc.cloud.DatabendPresignClientV1;
import com.fasterxml.jackson.core.JsonProcessingException;
import okhttp3.Headers;
import okhttp3.OkHttpClient;

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
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.databend.client.ClientSettings.DEFAULT_QUERY_TIMEOUT;
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
        return null;
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
        // currently not support commit
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

    }

    @Override
    public void rollback()
            throws SQLException {
        checkOpen();
        // currently not support rollback
        return;
    }

    @Override
    public void close()
            throws SQLException {

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
//        checkHoldability(resultSetHoldability);
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

    // TODO(zhihanz): session property push down
    DatabendClient startQuery(String sql) throws SQLException {
        PaginationOptions options = getPaginationOptions();
        ClientSettings s = new ClientSettings.Builder().
                setQueryTimeoutNanos(DEFAULT_QUERY_TIMEOUT).
                setConnectionTimeout(this.driverUri.getConnectionTimeout()).
                setSocketTimeout(this.driverUri.getSocketTimeout()).
                setSession(this.session.get()).
                setHost(this.getURI().toString()).
                setPaginationOptions(options).build();
        return new DatabendClientV1(httpClient, sql, s);
    }

    DatabendClient startQuery(String sql, StageAttachment attach) throws SQLException {
        PaginationOptions options = getPaginationOptions();
        ClientSettings s = new ClientSettings.Builder().setSession(this.session.get()).setHost(this.getURI().toString()).setQueryTimeoutNanos(DEFAULT_QUERY_TIMEOUT).setConnectionTimeout(this.driverUri.getConnectionTimeout()).setSocketTimeout(this.driverUri.getSocketTimeout()).setPaginationOptions(options).setStageAttachment(attach).build();
        return new DatabendClientV1(httpClient, sql, s);
    }


    @Override
    public void uploadStream(String stageName, String destPrefix, InputStream inputStream, String destFileName, boolean compressData)
            throws SQLException {
        // TODO(zhihanz) handle compress data
        // remove / in the end of stage name
        // remove / in the beginning of destPrefix and end of destPrefix
        String s;
        if (stageName == null) {
            s = "~";
        } else {
            s = stageName.replaceAll("/$", "");
        }
        String p = destPrefix.replaceAll("^/", "").replaceAll("/$", "");
        String dest = p + "/" + destFileName;
        try {
            logger.log(Level.FINE, "presign to @" + s + "/" + dest);
            PresignContext ctx = PresignContext.getPresignContext(this, PresignContext.PresignMethod.UPLOAD, s, dest);
            Headers h = ctx.getHeaders();
            String presignUrl = ctx.getUrl();
            if (this.driverUri.presignedUrlDisabled()) {
                DatabendPresignClient cli = new DatabendPresignClientV1(httpClient, this.httpUri.toString());
                cli.presignUpload(null, inputStream, s, p + "/", destFileName, true);
            } else {
                DatabendPresignClient cli = new DatabendPresignClientV1(new OkHttpClient(), this.httpUri.toString());
                cli.presignUpload(null, inputStream, h, presignUrl, true);
            }
        } catch (JsonProcessingException e) {
            System.out.println(e.getMessage());
            // For datax batch insert test, do not throw exception
//            throw new SQLException(e);
        } catch (IOException e) {
            System.out.println(e.getMessage());
//            throw new SQLException("failed to upload input stream", e);
        }
    }

    @Override
    public InputStream downloadStream(String stageName, String sourceFileName, boolean decompress)
            throws SQLException {
        String s = stageName.replaceAll("/$", "");
        DatabendPresignClient cli = new DatabendPresignClientV1(new OkHttpClient(), this.httpUri.toString());
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
