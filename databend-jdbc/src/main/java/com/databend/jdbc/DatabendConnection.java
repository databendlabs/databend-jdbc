package com.databend.jdbc;

import com.databend.jdbc.annotation.NotImplemented;
import com.databend.jdbc.cloud.DatabendCopyParams;
import com.databend.jdbc.exception.DatabendFailedToPingException;
import com.databend.jdbc.exception.DatabendSQLException;
import com.databend.jdbc.internal.query.QueryResultPages;
import com.databend.jdbc.internal.query.StageAttachment;
import com.databend.jdbc.internal.session.Capability;
import com.databend.jdbc.internal.session.DatabendSessionHandle;
import com.databend.jdbc.internal.session.QueryLiveness;
import com.databend.jdbc.internal.session.SessionHandleConfig;
import com.databend.jdbc.internal.session.SessionState;
import com.vdurmont.semver4j.Semver;
import okhttp3.*;

import java.io.File;
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
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;


public class DatabendConnection implements Connection, DatabendConnectionExtension, FileTransferAPI {

    private static final Logger logger = Logger.getLogger(DatabendConnection.class.getPackage().getName());

    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicBoolean autoCommit = new AtomicBoolean(true);
    private final AtomicReference<String> schema = new AtomicReference<>();
    private final ConcurrentHashMap<DatabendStatement, Boolean> statements = new ConcurrentHashMap<>();
    private final DatabendDriverUri driverUri;
    private final DatabendSessionHandle sessionHandle;

    private void initializeFileLogHandler() {
        if (this.debug()) {
            File file = new File("databend-jdbc-debug.log");
            if (!file.canWrite()) {
                logger.warning("No write access to file: " + file.getAbsolutePath());
                return;
            }
            try {
                // 2GB，Integer.MAX_VALUE
                System.setProperty("java.util.logging.FileHandler.limit", "2147483647");
                System.setProperty("java.util.logging.FileHandler.count", "200");
                // Enable log file reuse
                System.setProperty("java.util.logging.FileHandler.append", "true");
                FileHandler fileHandler= new FileHandler(file.getAbsolutePath(), Integer.parseInt(System.getProperty("java.util.logging.FileHandler.limit")),
                        Integer.parseInt(System.getProperty("java.util.logging.FileHandler.count")), true);
                fileHandler.setLevel(Level.ALL);
                fileHandler.setFormatter(new SimpleFormatter());
                logger.addHandler(fileHandler);
            } catch (Exception e) {
                throw new RuntimeException("Failed to create FileHandler", e);
            }
        }
    }


    DatabendConnection(DatabendDriverUri uri, OkHttpClient httpClient) throws SQLException {
        requireNonNull(uri, "uri is null");
        this.driverUri = uri;
        this.schema.set(uri.getDatabase());
        SessionHandleConfig config = this.driverUri.toSessionHandleConfig();
        this.sessionHandle = new DatabendSessionHandle(httpClient, config, this::queryLivenesses);

        initializeFileLogHandler();
        this.sessionHandle.login();
        this.sessionHandle.initializePresign(this.driverUri.getPresign(), this.driverUri.presignedUrlDisabled());
    }

    Semver getServerVersion() {
        return this.sessionHandle.getServerVersion();
    }

    Capability getServerCapability() {
        Semver version = this.sessionHandle.getServerVersion();
        return version == null ? null : new Capability(version);
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

    static String getCopyIntoSql(String database, DatabendCopyParams params) {
        StringBuilder sb = new StringBuilder();
        sb.append("COPY INTO ");
        if (database != null) {
            sb.append(database).append(".");
        }
        sb.append(params.getDatabaseTableName()).append(" ");
        sb.append("FROM ");
        sb.append(params.getDatabendStage().toString());
        sb.append(" ");
        sb.append(params);
        return sb.toString();
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

    synchronized private void registerStatement(DatabendStatement statement) {
        checkState(statements.put(statement, true) == null, "Statement is already registered");
    }

    synchronized private void unregisterStatement(DatabendStatement statement) {
        checkNotNull(statements.remove(statement), "Statement is not registered");
    }

    @Override
    public PreparedStatement prepareStatement(String s)
            throws SQLException {

        return this.prepareStatement(s, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
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
        SessionState currentSession = this.sessionHandle.getSession();
        if (currentSession != null) {
            currentSession.setAutoCommit(b);
        }
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
    }

    @Override
    public void close()
            throws SQLException {
        synchronized (this) {
            if (closed.get()) {
                return;
            }
            try {
                for (Statement stmt : new ArrayList<>(statements.keySet())) {
                    stmt.close();
                }
                this.sessionHandle.close();
            } finally {
                closed.set(true);
            }
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
        return Connection.TRANSACTION_NONE;
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
        DatabendPreparedStatement statement = new DatabendPreparedStatement(this, this::unregisterStatement, s);
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
    @Override
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
        this.startQuery("use " + schema);
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

    boolean copyPurge() {
        return this.driverUri.copyPurge();
    }

    Boolean useVerify() {
        return this.driverUri.getUseVerify();
    }

    Boolean debug() {
        return this.driverUri.getDebug();
    }

    String nullDisplay() {
        return this.driverUri.nullDisplay();
    }

    String binaryFormat() {
        return this.driverUri.binaryFormat();
    }

    public URI getURI() {
        return this.sessionHandle.getBaseUri();
    }

    void pingDatabendClientV1() throws SQLException {
        try (Statement statement = this.createStatement()) {
            statement.execute("select 1");
            ResultSet r = statement.getResultSet();
            while (r.next()) {
            }
        } catch (SQLException e) {
            throw new DatabendFailedToPingException(String.format("failed to ping databend server: %s", e.getMessage()));
        }
    }
    QueryResultPages startQuery(String sql) throws SQLException {
        return startQuery(sql, null);
    }

    QueryResultPages startQuery(String sql, StageAttachment attach) throws SQLException {
        String queryId = UUID.randomUUID().toString().replace("-", "");
        QueryResultPages queryPages;
        try {
            queryPages = sessionHandle.startQuery(queryId, sql, attach);
        } catch (RuntimeException e) {
            String message = e.getMessage() == null ? e.toString() : e.getMessage();
            throw new DatabendSQLException("Failed to start query: " + message, queryId, e);
        }
        return queryPages;
    }

    @Override
    public void uploadStream(InputStream inputStream, String stageName, String destPrefix, String destFileName, long fileSize, boolean compressData)
            throws SQLException {
        uploadStream(stageName, destPrefix, inputStream, destFileName, fileSize, compressData);
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
        this.sessionHandle.uploadStream(stageName, destPrefix, inputStream, destFileName, fileSize, compressData);
    }

    @Override
    public InputStream downloadStream(String stageName, String path)
            throws SQLException {
        return this.sessionHandle.downloadStream(stageName, path);
    }

    @Override
    public InputStream downloadStream(String stageName, String path, boolean decompress)
            throws SQLException {
        return downloadStream(stageName, path);
    }

    @Override
    public void copyIntoTable(String database, String tableName, DatabendCopyParams params)
            throws SQLException {
        DatabendCopyParams p = params == null ? DatabendCopyParams.builder().build() : params;
        requireNonNull(p.getDatabaseTableName(), "tableName is null");
        requireNonNull(p.getDatabendStage(), "stage is null");
        String sql = getCopyIntoSql(database, p);
        Statement statement = this.createStatement();
        statement.execute(sql);
        ResultSet rs = statement.getResultSet();
        while (rs.next()) {
        }
    }

    @Override
    public int loadStreamToTable(String sql, InputStream inputStream, long fileSize,  LoadMethod loadMethod) throws SQLException {
        if (!this.sessionHandle.supportsStreamingLoad()) {
            throw new SQLException("please upgrade databend-query to >1.2.781 to use loadStreamToTable, current version=" + this.sessionHandle.getServerVersion());
        }

        if (!sql.contains("@_databend_load")) {
            throw new SQLException("invalid sql: must contain @_databend_load when used in loadStreamToTable ");
        }

        if (loadMethod.equals(LoadMethod.STREAMING)) {
            return this.sessionHandle.streamingLoad(sql, inputStream, fileSize);
        } else {
            Instant now = Instant.now();
            long nanoTimestamp = now.getEpochSecond() * 1_000_000_000 + now.getNano();
            String fileName = String.valueOf(nanoTimestamp);
            String location = "~/_databend_load/" + fileName;
            sql = sql.replace("_databend_load", location);
            uploadStream("~", "_databend_load", inputStream, fileName, fileSize, false);
            Statement statement = this.createStatement();
            statement.execute(sql);
            ResultSet rs = statement.getResultSet();
            while (rs.next()) {
            }
            return statement.getUpdateCount();
        }
    }

    private List<QueryLiveness> queryLivenesses() {
        ArrayList<QueryLiveness> livenesses = new ArrayList<>();
        for (DatabendStatement stmt : statements.keySet()) {
            QueryLiveness queryLiveness = stmt.queryLiveness();
            if (queryLiveness != null) {
                livenesses.add(queryLiveness);
            }
        }
        return livenesses;
    }

    boolean isHeartbeatStopped() {
        return this.sessionHandle.isHeartbeatStopped();
    }
}
