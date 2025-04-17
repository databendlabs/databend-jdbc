package com.databend.jdbc;

import com.databend.client.ClientSettings;
import com.databend.client.DatabendClient;
import com.databend.client.DatabendClientV1;
import com.databend.client.DatabendSession;
import com.databend.client.ServerVersions;
import com.databend.client.PaginationOptions;
import com.databend.client.QueryRequest;
import com.databend.client.StageAttachment;
import com.databend.jdbc.annotation.NotImplemented;
import com.databend.jdbc.cloud.DatabendCopyParams;
import com.databend.jdbc.cloud.DatabendPresignClient;
import com.databend.jdbc.cloud.DatabendPresignClientV1;
import com.databend.jdbc.exception.DatabendFailedToPingException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.ConnectException;
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
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import java.util.zip.GZIPOutputStream;

import static com.databend.client.ClientSettings.*;
import static com.databend.client.DatabendClientV1.MEDIA_TYPE_JSON;
import static com.databend.client.DatabendClientV1.USER_AGENT_VALUE;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.net.URI.create;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;


public class DatabendConnection implements Connection, FileTransferAPI, Consumer<DatabendSession> {
    private static final Logger logger = Logger.getLogger(DatabendConnection.class.getPackage().getName());
    public static final String LOGOUT_PATH = "/v1/session/logout";
    public static final String HEARTBEAT_PATH = "/v1/session/heartbeat";
    private static FileHandler FILE_HANDLER;

    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicBoolean autoCommit = new AtomicBoolean(true);
    private final URI httpUri;
    private final AtomicReference<String> schema = new AtomicReference<>();
    private final OkHttpClient httpClient;
    private final ConcurrentHashMap<DatabendStatement, Boolean> statements = new ConcurrentHashMap();
    private final DatabendDriverUri driverUri;
    private boolean autoDiscovery;
    private AtomicReference<DatabendSession> session = new AtomicReference<>();

    private String routeHint = "";
    private AtomicReference<String> lastNodeID = new AtomicReference<>();

    static ExecutorService heartbeatScheduler = null;
    private HeartbeatManager heartbeatManager = new HeartbeatManager();

    private void initializeFileHandler() {
        if (this.debug()) {
            File file = new File("databend-jdbc-debug.log");
            if (!file.canWrite()) {
                logger.warning("No write access to file: " + file.getAbsolutePath());
                return;
            }
            try {
                System.setProperty("java.util.logging.FileHandler.limit", "2147483647"); // 2GB，Integer.MAX_VALUE
                System.setProperty("java.util.logging.FileHandler.count", "200");
                System.setProperty("java.util.logging.FileHandler.append", "true"); // Enable log file reuse
                FILE_HANDLER = new FileHandler(file.getAbsolutePath(), Integer.parseInt(System.getProperty("java.util.logging.FileHandler.limit")),
                        Integer.parseInt(System.getProperty("java.util.logging.FileHandler.count")), true);
                FILE_HANDLER.setLevel(Level.ALL);
                FILE_HANDLER.setFormatter(new SimpleFormatter());
                logger.addHandler(FILE_HANDLER);
            } catch (Exception e) {
                throw new RuntimeException("Failed to create FileHandler", e);
            }
        }
    }


    DatabendConnection(DatabendDriverUri uri, OkHttpClient httpClient) throws SQLException {
        requireNonNull(uri, "uri is null");
        // only used for presign url on non-object storage, which mainly served for demo pupose.
        // TODO: may also add query id and load balancing on the part.
        this.httpUri = uri.getUri();
        this.httpClient = httpClient;
        this.driverUri = uri;
        this.schema.set(uri.getDatabase());
        this.routeHint = randRouteHint();
        // it maybe closed due to unsupported server versioning.
        this.autoDiscovery = uri.autoDiscovery();
        DatabendSession session = new DatabendSession.Builder().setDatabase(this.getSchema()).setSettings(uri.getSessionSettings()).build();
        this.setSession(session);

        initializeFileHandler();
    }

    public static String randRouteHint() {
        String charset = "abcdef0123456789";
        Random rand = new Random();
        StringBuilder sb = new StringBuilder(16);
        for (int i = 0; i < 16; i++) {
            sb.append(charset.charAt(rand.nextInt(charset.length())));
        }
        return sb.toString();
    }

    private static final char SPECIAL_CHAR = '#';

    public static String uriRouteHint(String URI) {
        // Encode the URI using Base64
        String encodedUri = Base64.getEncoder().encodeToString(URI.getBytes());

        // Append the special character
        return encodedUri + SPECIAL_CHAR;
    }

    public static URI parseRouteHint(String routeHint) {
        if (routeHint == null || routeHint.isEmpty()) {
            return null;
        }
        URI target;
        try {
            if (routeHint.charAt(routeHint.length() - 1) != SPECIAL_CHAR) {
                return null;
            }
            // Remove the special character
            String encodedUri = routeHint.substring(0, routeHint.length() - 1);

            // Decode the Base64 string
            byte[] decodedBytes = Base64.getDecoder().decode(encodedUri);
            String decodedUri = new String(decodedBytes);

            return create(decodedUri);
        } catch (Exception e) {
            logger.log(Level.FINE, "Failed to parse route hint: " + routeHint, e);
            return null;
        }
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

    public boolean inActiveTransaction() {
        if (this.session.get() == null) {
            return false;
        }
        return this.session.get().inActiveTransaction();
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

    synchronized private void registerStatement(DatabendStatement statement) {
        checkState(statements.put(statement, true) == null, "Statement is already registered");
    }

    synchronized private void unregisterStatement(DatabendStatement statement) {
        checkNotNull(statements.remove(statement), "Statement is not registered");
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
        for (Statement stmt : statements.keySet()) {
            stmt.close();
        }
        logout();
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

    public int getMaxFailoverRetries() {
        return this.driverUri.getMaxFailoverRetry();
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

    public boolean presignedUrlDisabled() {
        return this.driverUri.presignedUrlDisabled();
    }

    public boolean copyPurge() {
        return this.driverUri.copyPurge();
    }

    public boolean isAutoDiscovery() {
        return this.autoDiscovery;
    }

    public String warehouse() {
        return this.driverUri.getWarehouse();
    }

    public Boolean strNullAsNull() {
        return this.driverUri.getStrNullAsNull();
    }

    public Boolean useVerify() {
        return this.driverUri.getUseVerify();
    }

    public Boolean debug() {
        return this.driverUri.getDebug();
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
        try (Statement statement = this.createStatement()) {
            statement.execute("select 1");
            ResultSet r = statement.getResultSet();
            while (r.next()) {
                //System.out.println(r.getInt(1));
            }
        } catch (SQLException e) {
            throw new DatabendFailedToPingException(String.format("failed to ping databend server: %s", e.getMessage()));
        }
    }

    public void accept(DatabendSession session) {
        setSession(session);
    }

    /**
     * Retry executing a query in case of connection errors. fail over mechanism is used to retry the query when connect error occur
     * It will find next target host based on configured Load balancing Policy.
     *
     * @param sql The SQL statement to execute.
     * @param attach The stage attachment to use for the query.
     * @return A DatabendClient instance representing the successful query execution.
     * @throws SQLException If the query fails after retrying the specified number of times.
     * @see DatabendClientLoadBalancingPolicy
     */
    DatabendClient startQueryWithFailover(String sql, StageAttachment attach) throws SQLException {
        int maxRetries = getMaxFailoverRetries();
        SQLException lastException = null;

        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            try {
                String queryId = UUID.randomUUID().toString();
                String candidateHost = selectHostForQuery(queryId);

                // configure the client settings
                ClientSettings.Builder sb = this.makeClientSettings(queryId, candidateHost);
                if (attach != null) {
                    sb.setStageAttachment(attach);
                }
                ClientSettings settings = sb.build();

                logger.log(Level.FINE, "execute query #{0}: SQL: {1} host: {2}",
                        new Object[]{attempt + 1, sql, settings.getHost()});

                // need to retry the auto discovery in case of connection error
                if (this.autoDiscovery) {
                    tryAutoDiscovery(httpClient, settings);
                }

                return new DatabendClientV1(httpClient, sql, settings, this, lastNodeID);
            } catch (Exception e) {
                // handle the exception and retry the query
                if (shouldRetryException(e) && attempt < maxRetries) {
                    lastException = wrapException("query failed", sql, e);
                    try {
                        Thread.sleep(Math.min(100 * (1 << attempt), 5000)); // back off retry
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw wrapException("query interrupt", sql, ie);
                    }
                } else {
                    // throw the exception
                    if (e instanceof SQLException) {
                        throw (SQLException) e;
                    } else {
                        throw wrapException("Query failed，no need to retry", sql, e);
                    }
                }
            }
        }

        throw new SQLException("after" + maxRetries + "times retry and failed: SQL: " + sql, lastException);
    }

    private boolean shouldRetryException(Exception e) {
        Throwable cause = e.getCause();
        // connection error
        if (cause instanceof ConnectException) {
            return true;
        }

        if (e instanceof RuntimeException) {
            String message = e.getMessage();
            return message != null && (
                    message.contains("520") ||
                            message.contains("timeout") ||
                            message.contains("retry")
            );
        }

        return false;
    }

    private String selectHostForQuery(String queryId) {
        String candidateHost = this.driverUri.getUri(queryId).toString();

        if (!inActiveTransaction()) {
            this.routeHint = uriRouteHint(candidateHost);
        }

        if (this.routeHint != null && !this.routeHint.isEmpty()) {
            URI uri = parseRouteHint(this.routeHint);
            if (uri != null) {
                candidateHost = uri.toString();
            }
        }

        return candidateHost;
    }

    private SQLException wrapException(String prefix, String sql, Exception e) {
        String message = prefix + ": SQL: " + sql;
        if (e.getMessage() != null) {
            message += " - " + e.getMessage();
        }
        if (e.getCause() != null) {
            message += " (Reason: " + e.getCause().getMessage() + ")";
        }
        return new SQLException(message, e);
    }
    /**
     * Try to auto discovery the databend nodes it will log exceptions when auto discovery failed and not affect real query execution
     *
     * @param client the http client to query on
     * @param settings the client settings to use
     */
    void tryAutoDiscovery(OkHttpClient client, ClientSettings settings) {
        if (this.autoDiscovery) {
            if (this.driverUri.enableMock()) {
                settings.getAdditionalHeaders().put("~mock.unsupported.discovery", "true");
            }
            DatabendNodes nodes = this.driverUri.getNodes();
            if (nodes != null && nodes.needDiscovery()) {
                try {
                    nodes.discoverUris(client, settings);
                } catch (UnsupportedOperationException e) {
                    logger.log(Level.WARNING, "Current Query Node do not support auto discovery, close the functionality: " + e.getMessage());
                    this.autoDiscovery = false;
                } catch (Exception e) {
                    logger.log(Level.FINE, "Error auto discovery: " + " cause: " + e.getCause() + " message: " + e.getMessage());
                }
            }
        }

    }

    DatabendClient startQuery(String sql) throws SQLException {
        return startQuery(sql, null);
    }

    DatabendClient startQuery(String sql, StageAttachment attach) throws SQLException {
        DatabendClient client = startQueryWithFailover(sql, attach);
        Long timeout = client.getResults().getResultTimeoutSecs();
        if (timeout != null && timeout != 0) {
            heartbeatManager.onStartQuery(timeout);
        }
        return client;
    }

    private ClientSettings.Builder makeClientSettings(String queryID, String host) {
        PaginationOptions options = getPaginationOptions();
        Map<String, String> additionalHeaders = setAdditionalHeaders();
        additionalHeaders.put(X_Databend_Query_ID, queryID);
        return new Builder().
                setSession(this.session.get()).
                setHost(host).
                setQueryTimeoutSecs(this.driverUri.getQueryTimeout()).
                setConnectionTimeout(this.driverUri.getConnectionTimeout()).
                setSocketTimeout(this.driverUri.getSocketTimeout()).
                setPaginationOptions(options).
                setAdditionalHeaders(additionalHeaders);
    }

    private Map<String, String> setAdditionalHeaders() {
        Map<String, String> additionalHeaders = new HashMap<>();

        DatabendSession session = this.getSession();
        String warehouse = null;
        if (session != null ) {
            Map<String, String> settings = session.getSettings();
            if (settings != null) {
                warehouse = settings.get("warehouse");
            }
        }
        if (warehouse == null && !this.driverUri.getWarehouse().isEmpty()) {
            warehouse = this.driverUri.getWarehouse();
        }
        if (warehouse!=null) {
            additionalHeaders.put(DatabendWarehouseHeader, warehouse);
        }

        if (!this.driverUri.getTenant().isEmpty()) {
            additionalHeaders.put(DatabendTenantHeader, this.driverUri.getTenant());
        }
        if (!this.routeHint.isEmpty()) {
            additionalHeaders.put(X_DATABEND_ROUTE_HINT, this.routeHint);
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
//                logger.log(Level.FINE, "presign to @" + s + "/" + dest);
                long presignStartTime = System.nanoTime();
                PresignContext ctx = PresignContext.getPresignContext(this, PresignContext.PresignMethod.UPLOAD, s, dest);
                long presignEndTime = System.nanoTime();
                if (this.debug()) {
                    logger.info("presign cost time: " + (presignEndTime - presignStartTime) / 1000000.0 + "ms");
                }
                Headers h = ctx.getHeaders();
                String presignUrl = ctx.getUrl();
                DatabendPresignClient cli = new DatabendPresignClientV1(new OkHttpClient(), this.httpUri.toString());
                long uploadStartTime = System.nanoTime();
                cli.presignUpload(null, dataStream, h, presignUrl, fileSize, true);
                long uploadEndTime = System.nanoTime();
                if (this.debug()) {
                    logger.info("upload cost time: " + (uploadEndTime - uploadStartTime) / 1000000.0 + "ms");
                }
            }
        } catch (RuntimeException e) {
            System.out.println(e.getMessage());
            throw new SQLException(e);
        } catch (IOException e) {
            logger.warning("failed to upload input stream, file size is:" + fileSize / 1024.0 + e.getMessage());
            throw new SQLException(e);
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
        } catch (RuntimeException e) {
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
    void logout() throws SQLException {
        DatabendSession session = this.session.get();
        if (session == null || !session.getNeedKeepAlive()) {
            return;
        }
        generalRequest(LOGOUT_PATH, "{}");
    }

    String generalRequest(String path, String body)  throws SQLException {
        DatabendSession session = this.session.get();
        int times = getMaxFailoverRetries() + 1;
        List hosts = new LinkedList<String>();
        String failReason = null;
        String lastHost = null;

        for (int i = 1; i <= times; i++) {
            String candidateHost = this.driverUri.getUri("").toString();
            // candidateHost = "http://localhost:8888";
            hosts.add(candidateHost);
            if (lastHost == candidateHost) {
                break;
            }
            lastHost = candidateHost;
            logger.log(Level.FINE, "retry " + i + " times to logout on " + candidateHost);

            ClientSettings settings = this.makeClientSettings("", candidateHost).build();
            HttpUrl url = HttpUrl.get(candidateHost).newBuilder().encodedPath(path).build();
            Request.Builder builder = new Request.Builder()
                    .url(url)
                    .header("User-Agent", USER_AGENT_VALUE);
            if (settings.getAdditionalHeaders() != null) {
                settings.getAdditionalHeaders().forEach(builder::addHeader);
            }
            if (session.getNeedSticky()) {
                builder.addHeader(ClientSettings.X_DATABEND_ROUTE_HINT, uriRouteHint(candidateHost));
                String lastNodeID = this.lastNodeID.get();
                if (lastNodeID != null)
                    builder.addHeader(ClientSettings.X_DATABEND_STICKY_NODE, lastNodeID);
            }
            for (int j = 1; j <= 3; j++) {
                Request request = builder.post(okhttp3.RequestBody.create(MEDIA_TYPE_JSON, body)).build();
                try (Response response = httpClient.newCall(request).execute()) {
                    if (response.code() != 200) {
                        throw new SQLException("Error logout: code =" + response.code() + ", body = " + response.body());
                    }
                    return response.body().string();
                } catch (IOException e) {
                    System.out.println("e = " + e.getMessage());
                    if (e.getCause() instanceof ConnectException) {
                        if (failReason == null) {
                            failReason = e.getMessage();
                        }
                        try {
                            MILLISECONDS.sleep(j * 100);
                        } catch (InterruptedException e2) {
                            Thread.currentThread().interrupt();
                            return null;
                        }
                    } else {
                       break;
                    }
                }
            }
        }
        throw new SQLException("Failover Retry Error executing query after retries on hosts " + hosts + ": " + failReason);
    }

    class HeartbeatManager implements Runnable {
        private ScheduledFuture<?> heartbeatFuture;
        private long heartbeatIntervalMillis = 30000;
        private long lastHeartbeatStartTimeMillis = 0;
        private ScheduledExecutorService getScheduler() {
            if (heartbeatScheduler == null) {
                synchronized (HeartbeatManager.class) {
                    if (heartbeatScheduler == null) {
                        // create daemon thread so that it will not block JVM from exiting.
                        heartbeatScheduler =
                                Executors.newScheduledThreadPool(
                                        1,
                                        runnable -> {
                                            Thread thread = Executors.defaultThreadFactory().newThread(runnable);
                                            thread.setName("heartbeat (" + thread.getId() + ")");
                                            thread.setDaemon(true);
                                            return thread;
                                        });
                    }
                }
            }
            return (ScheduledExecutorService) heartbeatScheduler;
        }

        private void scheduleHeartbeat() {
            long delay =  Math.max(heartbeatIntervalMillis - (System.currentTimeMillis() - lastHeartbeatStartTimeMillis), 0);
            heartbeatFuture = getScheduler().schedule(this, delay, MILLISECONDS);
        }

        private ArrayList<QueryLiveness> queryLiveness() {
            ArrayList<QueryLiveness> arr = new ArrayList<>();
            for (DatabendStatement stmt : statements.keySet()) {
                QueryLiveness ql = stmt.queryLiveness();
                if (ql != null && !ql.stopped && ServerVersions.supportHeartbeat(ql.serverVersion)) {
                    arr.add(ql);
                }
            }
            return arr;
        }

        private void doHeartbeat(ArrayList<QueryLiveness> queryLivenesses ) {
            long now = System.currentTimeMillis();
            lastHeartbeatStartTimeMillis = now;
            Map<String, ArrayList<String>> nodeToQueryID = new HashMap();
            Map<String, QueryLiveness> queries = new HashMap();

            for (QueryLiveness ql: queryLivenesses) {
                if (now - ql.lastRequestTime.get() >= ql.resultTimeoutSecs * 1000 / 2) {
                    nodeToQueryID.computeIfAbsent(ql.nodeID, k -> new ArrayList()).add(ql.queryID);
                    queries.put(ql.queryID, ql);
                }
            }
            if (nodeToQueryID.isEmpty())
                return;

            ObjectMapper mapper = new ObjectMapper();
            Map<String, Object> map = new HashMap<>();
            map.put("node_to_queries", nodeToQueryID);

            try {
                String body = mapper.writeValueAsString(map);

                body = generalRequest(HEARTBEAT_PATH, body);
                JsonNode toRemove = mapper.readTree(body).get("queries_to_remove");
                if (toRemove.isArray()) {
                    for (JsonNode element : toRemove) {
                        String queryId = element.asText();
                        queries.get(queryId).stopped = true;
                    }
                }
            } catch (JsonProcessingException e) {
                logger.warning("fail to encode heartbeat body: " + e);
            } catch (SQLException e) {
                logger.warning("fail to send heartbeat: " + e);
            } catch (IOException e) {
                logger.warning("fail to send heartbeat: " + e);
                throw new RuntimeException(e);
            }
        }

        public void onStartQuery(Long timeoutSecs) {
            synchronized (DatabendConnection.this) {
                if (timeoutSecs * 1000 / 4 < heartbeatIntervalMillis) {
                    heartbeatIntervalMillis = timeoutSecs * 1000 / 4;
                    if (heartbeatFuture != null) {
                        heartbeatFuture.cancel(false);
                        heartbeatFuture = null;
                    }
                }
                if (heartbeatFuture == null)
                    scheduleHeartbeat();
            }
        }

        @Override
        public void run() {
            ArrayList<QueryLiveness> arr = queryLiveness();
            doHeartbeat(arr);

            synchronized (DatabendConnection.this) {
                heartbeatFuture = null;
                if (arr.size() > 0) {
                    if (heartbeatFuture == null)
                        scheduleHeartbeat();
                } else {
                    heartbeatFuture = null;
                }
            }
        }
    }

    boolean isHeartbeatStopped() {
        return heartbeatManager.heartbeatFuture == null;
    }
}

