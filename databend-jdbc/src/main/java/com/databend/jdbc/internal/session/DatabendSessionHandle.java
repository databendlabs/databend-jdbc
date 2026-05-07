package com.databend.jdbc.internal.session;

import com.databend.jdbc.internal.QueryResultFormat;
import com.databend.jdbc.internal.exception.DatabendPresignException;
import com.databend.jdbc.internal.exception.DatabendQueryException;
import com.databend.jdbc.internal.exception.DatabendSessionException;
import com.databend.jdbc.internal.exception.DatabendStageUploadException;
import com.databend.jdbc.internal.exception.DatabendStreamingLoadException;
import com.databend.jdbc.internal.http.PresignClient;
import com.databend.jdbc.internal.http.HttpRetryPolicy;
import com.databend.jdbc.internal.http.JsonCodec;
import com.databend.jdbc.internal.http.RetryableHttpStatusException;
import com.databend.jdbc.internal.query.QueryResultPages;
import com.databend.jdbc.internal.query.QueryResults;
import com.databend.jdbc.internal.query.RestQueryResultPages;
import com.databend.jdbc.internal.query.StageAttachment;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.vdurmont.semver4j.Semver;
import okhttp3.Headers;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.MultipartBody;
import okhttp3.OkHttpClient;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okio.BufferedSink;
import okio.Okio;
import okio.Source;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.logging.Logger;
import java.util.zip.GZIPOutputStream;

import static com.databend.jdbc.internal.http.JsonCodec.jsonCodec;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class DatabendSessionHandle implements Consumer<SessionState> {
    private static final Logger logger = Logger.getLogger(DatabendSessionHandle.class.getPackage().getName());
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final JsonCodec<SessionState> SESSION_JSON_CODEC = jsonCodec(SessionState.class);
    private static final String STREAMING_LOAD_PATH = "/v1/streaming_load";
    private static final String UPLOAD_TO_STAGE_PATH = "/v1/upload_to_stage";
    private static final String LOGIN_PATH = "/v1/session/login";
    private static final String LOGOUT_PATH = "/v1/session/logout";
    private static final String HEARTBEAT_PATH = "/v1/session/heartbeat";
    private static final MediaType MEDIA_TYPE_JSON = MediaType.parse("application/json; charset=utf-8");
    private static final Semver STREAMING_LOAD_MIN_VERSION = new Semver("1.2.781");
    private static final Semver HEARTBEAT_MIN_VERSION = new Semver("1.2.709");
    private static final int MIN_ARROW_RESULT_VERSION = 3;
    private static final int MAX_STAGE_UPLOAD_RETRY_ATTEMPTS = 20;
    private static final Duration STAGE_UPLOAD_RETRY_TIMEOUT = Duration.ofMinutes(5);

    private static volatile ExecutorService heartbeatScheduler = null;

    private final OkHttpClient httpClient;
    private final SessionHandleConfig config;
    private final AtomicReference<SessionState> session;
    private final AtomicReference<String> lastNodeID = new AtomicReference<>();
    private final Supplier<List<QueryLiveness>> queryLivenessSupplier;
    private final HeartbeatManager heartbeatManager = new HeartbeatManager();
    private volatile String routeHint;
    private volatile Semver serverVersion;
    private volatile Integer serverMaxArrowResultVersion;
    private volatile boolean presignDisabled;

    public DatabendSessionHandle(
            OkHttpClient httpClient,
            SessionHandleConfig config,
            Supplier<List<QueryLiveness>> queryLivenessSupplier) {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.config = requireNonNull(config, "config is null");
        this.session = new AtomicReference<>(requireNonNull(config.getInitialSession(), "config.initialSession is null"));
        this.queryLivenessSupplier = queryLivenessSupplier != null ? queryLivenessSupplier : java.util.Collections::emptyList;
        this.routeHint = "";
    }

    public void login() throws SQLException {
        HttpRetryPolicy retryPolicy = new HttpRetryPolicy(true, true);
        Map<String, String> headers = new HashMap<>();
        headers.put("Accept", "application/json");
        headers.put("Content-Type", "application/json");

        LoginRequest request = new LoginRequest();
        SessionState currentSession = this.session.get();
        request.database = currentSession != null ? currentSession.getDatabase() : null;
        request.settings = currentSession != null ? currentSession.getSettings() : null;

        try {
            String bodyString = objectMapper.writeValueAsString(request);
            RequestBody requestBody = RequestBody.create(MEDIA_TYPE_JSON, bodyString);
            HttpRetryPolicy.ResponseWithBody response =
                    requestHelper(LOGIN_PATH, HttpMethod.POST, requestBody, headers, retryPolicy);

            // old server does not support this API
            if (response.statusCode != 400) {
                try {
                    JsonNode json = objectMapper.readTree(response.body);
                    JsonNode versionNode = json.get("version");
                    if (versionNode != null && !versionNode.isNull()) {
                        this.serverVersion = new Semver(versionNode.asText());
                    }
                    JsonNode serverMaxArrowResultVersionNode = json.get("server_max_arrow_result_version");
                    if (serverMaxArrowResultVersionNode != null && !serverMaxArrowResultVersionNode.isNull()) {
                        this.serverMaxArrowResultVersion = serverMaxArrowResultVersionNode.asInt();
                    }
                } catch (IOException | IllegalArgumentException e) {
                    throw new DatabendSessionException("Failed to decode login response", e);
                }
            }
        } catch (IOException e) {
            throw new DatabendSessionException("Failed to encode login request", e);
        }
    }

    public void initializePresign(String presign, boolean legacyPresignDisabled) {
        if (presign == null || presign.isEmpty()) {
            this.presignDisabled = legacyPresignDisabled;
            return;
        }

        switch (presign.toLowerCase(Locale.US)) {
            case "auto":
                String host = this.config.getBaseUri().getHost();
                this.presignDisabled = host == null
                        || (!host.endsWith(".databend.com")
                        && !host.endsWith(".databend.cn")
                        && !host.endsWith(".tidbcloud.com"));
                break;
            case "detect":
                try {
                    getPresignedRequest(PresignMethod.UPLOAD, "~", ".databend-jdbc/check");
                    this.presignDisabled = false;
                } catch (Exception e) {
                    logger.warning("presign off: detect failed: " + e.getMessage());
                    this.presignDisabled = true;
                }
                break;
            case "on":
                this.presignDisabled = false;
                break;
            case "off":
                this.presignDisabled = true;
                break;
            default:
                logger.warning("Unknown presign value: " + presign + ", falling back to presigned_url_disabled");
                this.presignDisabled = legacyPresignDisabled;
                break;
        }

        if (this.config.isDebug()) {
            logger.info("presign disabled: " + this.presignDisabled
                    + " (presign=" + presign + ", host=" + this.config.getBaseUri().getHost() + ")");
        }
    }

    public void close() throws SQLException {
        heartbeatManager.stop();
        logout();
    }

    public QueryResultPages startQuery(String sql) throws SQLException {
        return startQuery(sql, null);
    }

    public QueryResultPages startQuery(String sql, StageAttachment attach) throws SQLException {
        String queryId = UUID.randomUUID().toString().replace("-", "");
        return startQuery(queryId, sql, attach, null);
    }

    public QueryResultPages startQuery(String queryId, String sql, StageAttachment attach) throws SQLException {
        return startQuery(queryId, sql, attach, null);
    }

    public QueryResultPages startQuery(String queryId, String sql, StageAttachment attach, QueryResultFormat queryResultFormatOverride) throws SQLException {
        SessionState currentSession = this.session.get();
        if (currentSession == null || !currentSession.inActiveTransaction()) {
            this.routeHint = uriRouteHint(this.config.getBaseUri().toString());
        }
        QueryRequestConfig.Builder builder = makeRequestConfig(queryId, this.config.getBaseUri().toString(), queryResultFormatOverride);
        if (attach != null) {
            builder.setStageAttachment(attach);
        }
        QueryRequestConfig requestConfig = builder.build();
        try {
            QueryResultPages pages = new RestQueryResultPages(httpClient, sql, requestConfig, this, lastNodeID);
            Long timeout = pages.getResults().getResultTimeoutSecs();
            if (timeout != null && timeout != 0) {
                heartbeatManager.onStartQuery(timeout);
            }
            return pages;
        } catch (RuntimeException e) {
            String message = e.getMessage() == null ? e.toString() : e.getMessage();
            throw new DatabendQueryException("Failed to start query: " + message, e);
        }
    }

    public int streamingLoad(String sql, InputStream inputStream, long fileSize) throws SQLException {
        HttpRetryPolicy retryPolicy = new HttpRetryPolicy(true, true);

        try {
            Map<String, String> headers = new HashMap<>();
            SessionState currentSession = this.session.get();
            if (currentSession != null) {
                String sessionString = objectMapper.writeValueAsString(currentSession);
                headers.put(QueryRequestConfig.DATABEND_QUERY_CONTEXT_HEADER, sessionString);
            }
            headers.put(QueryRequestConfig.DATABEND_SQL_HEADER, sql);
            headers.put("Accept", "application/json");
            RequestBody requestBody = buildMultipartBody(inputStream, fileSize);
            HttpRetryPolicy.ResponseWithBody response =
                    requestHelper(STREAMING_LOAD_PATH, HttpMethod.PUT, requestBody, headers, retryPolicy);
            try {
                JsonNode json = objectMapper.readTree(response.body);
                JsonNode error = json.get("error");
                if (error != null) {
                    throw new SQLException(
                            "streaming load fail: code = " + error.get("code").asText()
                                    + ", message=" + error.get("message").asText());
                }

                String encodedSession = response.headers.get(QueryRequestConfig.DATABEND_QUERY_CONTEXT_HEADER);
                if (encodedSession != null) {
                    byte[] bytes = Base64.getUrlDecoder().decode(encodedSession);
                    String sessionJson = new String(bytes);
                    SessionState updatedSession = SESSION_JSON_CODEC.fromJson(sessionJson);
                    if (updatedSession != null) {
                        this.session.set(updatedSession);
                    }
                }

                JsonNode stats = json.get("stats");
                if (stats != null) {
                    int rows = stats.get("rows").asInt(-1);
                    if (rows != -1) {
                        return rows;
                    }
                }
                throw new SQLException("invalid response for " + STREAMING_LOAD_PATH + ": " + response.bodyString());
            } catch (IOException | IllegalArgumentException e) {
                throw new DatabendStreamingLoadException("Failed to decode streaming load response", e);
            }
        } catch (SQLException e) {
            if (e.getCause() instanceof StreamingLoadRequestEncodingFailure) {
                Throwable cause = e.getCause().getCause() != null ? e.getCause().getCause() : e.getCause();
                throw new DatabendStreamingLoadException("Failed to encode streaming load request", cause);
            }
            throw e;
        } catch (IOException e) {
            throw new DatabendStreamingLoadException("Failed to encode streaming load request", e);
        }
    }

    public void uploadStream(
            String stageName,
            String destPrefix,
            InputStream inputStream,
            String destFileName,
            long fileSize,
            boolean compressData) throws SQLException {
        String normalizedStage = stageName == null ? "~" : stageName.replaceAll("/$", "");
        String normalizedPrefix = destPrefix.replaceAll("^/", "").replaceAll("/$", "");
        String destination = normalizedPrefix + "/" + destFileName;

        try {
            InputStream dataStream = inputStream;
            if (compressData) {
                ByteArrayOutputStream buffer = new ByteArrayOutputStream();
                try (GZIPOutputStream gzipOutputStream = new GZIPOutputStream(buffer)) {
                    byte[] bytes = new byte[1024];
                    int len;
                    while ((len = inputStream.read(bytes)) != -1) {
                        gzipOutputStream.write(bytes, 0, len);
                    }
                }
                dataStream = new ByteArrayInputStream(buffer.toByteArray());
                fileSize = buffer.size();
            }

            if (this.presignDisabled) {
                uploadToStage(normalizedStage, normalizedPrefix + "/", destFileName, dataStream, fileSize);
                return;
            }

            PresignedRequestContext presigned;
            try {
                presigned = getPresignedRequest(PresignMethod.UPLOAD, normalizedStage, destination);
            } catch (RuntimeException e) {
                throw new SQLException("Failed to prepare presigned upload request", e);
            }
            PresignClient client = new PresignClient();
            try {
                client.presignUpload(null, dataStream, presigned.headers, presigned.url, fileSize, true);
            } catch (RuntimeException | IOException e) {
                throw new SQLException(
                        "Failed to upload stream",
                        new DatabendPresignException("Failed to upload via presigned request", e));
            }
        } catch (DatabendStageUploadException e) {
            logger.warning("failed to upload input stream, file size is:" + fileSize / 1024.0 + e.getMessage());
            throw new SQLException("Failed to upload stream", e);
        } catch (DatabendPresignException e) {
            logger.warning("failed to upload input stream, file size is:" + fileSize / 1024.0 + e.getMessage());
            throw new SQLException("Failed to upload stream", e);
        } catch (IOException e) {
            logger.warning("failed to upload input stream, file size is:" + fileSize / 1024.0 + e.getMessage());
            throw new SQLException("Failed to upload stream", e);
        }
    }

    private void uploadToStage(
            String stageName,
            String relativePath,
            String fileName,
            InputStream inputStream,
            long fileSize) throws IOException {
        RequestBody requestBody = new MultipartBody.Builder()
                .setType(MultipartBody.FORM)
                .addFormDataPart("upload", fileName, new StageUploadRequestBody(inputStream, fileSize))
                .build();

        Request.Builder builder = new Request.Builder()
                .url(buildUrl(UPLOAD_TO_STAGE_PATH))
                .addHeader(QueryRequestConfig.X_DATABEND_STAGE_NAME, stageName)
                .addHeader(QueryRequestConfig.X_DATABEND_RELATIVE_PATH, relativePath)
                .put(requestBody);
        if (this.config.getWarehouse() != null && !this.config.getWarehouse().isEmpty()) {
            builder.addHeader(QueryRequestConfig.DATABEND_WAREHOUSE_HEADER, this.config.getWarehouse());
        }

        executeStageUpload(builder.build());
    }

    public InputStream downloadStream(String stageName, String path) throws SQLException {
        String normalizedStage = stageName.replaceAll("/$", "");
        PresignClient client = new PresignClient();
        PresignedRequestContext presigned;
        try {
            presigned = getPresignedRequest(PresignMethod.DOWNLOAD, normalizedStage, path);
        } catch (RuntimeException e) {
            throw new SQLException("Failed to prepare presigned download request", e);
        }
        try {
            return client.presignDownloadStream(presigned.headers, presigned.url);
        } catch (RuntimeException e) {
            throw new SQLException(
                    "Failed to open presigned download stream",
                    new DatabendPresignException("Failed to open presigned download stream", e));
        }
    }

    public PaginationOptions getPaginationOptions() {
        PaginationOptions.Builder builder = PaginationOptions.builder();
        builder.setWaitTimeSecs(this.config.getWaitTimeSecs());
        builder.setMaxRowsInBuffer(this.config.getMaxRowsInBuffer());
        builder.setMaxRowsPerPage(this.config.getMaxRowsPerPage());
        return builder.build();
    }

    public Map<String, String> newAdditionalHeaders() {
        Map<String, String> additionalHeaders = new HashMap<>();
        SessionState currentSession = this.session.get();
        String warehouse = null;
        if (currentSession != null && currentSession.getSettings() != null) {
            warehouse = currentSession.getSettings().get("warehouse");
        }
        if (warehouse == null && this.config.getWarehouse() != null && !this.config.getWarehouse().isEmpty()) {
            warehouse = this.config.getWarehouse();
        }
        if (warehouse != null) {
            additionalHeaders.put(QueryRequestConfig.DATABEND_WAREHOUSE_HEADER, warehouse);
        }
        if (this.config.getTenant() != null && !this.config.getTenant().isEmpty()) {
            additionalHeaders.put(QueryRequestConfig.DATABEND_TENANT_HEADER, this.config.getTenant());
        }
        if (this.routeHint != null && !this.routeHint.isEmpty()) {
            additionalHeaders.put(QueryRequestConfig.X_DATABEND_ROUTE_HINT, this.routeHint);
        }
        return additionalHeaders;
    }

    public SessionState getSession() {
        return this.session.get();
    }

    public Semver getServerVersion() {
        return this.serverVersion;
    }

    public boolean supportsStreamingLoad() {
        return this.serverVersion != null && this.serverVersion.isGreaterThan(STREAMING_LOAD_MIN_VERSION);
    }

    public boolean supportsHeartBeat() {
        return this.serverVersion != null && this.serverVersion.isGreaterThan(HEARTBEAT_MIN_VERSION);
    }

    public URI getBaseUri() {
        return this.config.getBaseUri();
    }

    public boolean isPresignDisabled() {
        return this.presignDisabled;
    }

    public boolean isHeartbeatStopped() {
        return heartbeatManager.isStopped();
    }

    @Override
    public void accept(SessionState session) {
        if (session != null) {
            this.session.set(session);
        }
    }

    private QueryRequestConfig.Builder makeRequestConfig(String queryId, String host) {
        return makeRequestConfig(queryId, host, null);
    }

    private QueryRequestConfig.Builder makeRequestConfig(String queryId, String host, QueryResultFormat queryResultFormatOverride) {
        Map<String, String> additionalHeaders = newAdditionalHeaders();
        additionalHeaders.put(QueryRequestConfig.X_DATABEND_QUERY_ID, queryId);
        QueryResultFormat queryResultFormat = queryResultFormatOverride == null
                ? this.config.getQueryResultFormat()
                : queryResultFormatOverride;
        if (queryResultFormat == QueryResultFormat.ARROW && !supportsArrowTransport()) {
            queryResultFormat = QueryResultFormat.JSON;
        }
        return QueryRequestConfig.builder()
                .setSession(this.session.get())
                .setHost(host)
                .setQueryTimeoutSecs(this.config.getQueryTimeoutSecs())
                .setConnectionTimeout(this.config.getConnectionTimeoutSecs())
                .setSocketTimeout(this.config.getSocketTimeoutSecs())
                .setQueryResultFormat(queryResultFormat)
                .setPaginationOptions(getPaginationOptions())
                .setAdditionalHeaders(additionalHeaders);
    }

    private boolean supportsArrowTransport() {
        return this.serverMaxArrowResultVersion != null && this.serverMaxArrowResultVersion >= MIN_ARROW_RESULT_VERSION;
    }

    private void logout() throws SQLException {
        SessionState currentSession = this.session.get();
        if (currentSession == null || !currentSession.getNeedKeepAlive()) {
            return;
        }
        HttpRetryPolicy retryPolicy = new HttpRetryPolicy(false, false);
        RequestBody body = RequestBody.create(MEDIA_TYPE_JSON, "{}");
        requestHelper(LOGOUT_PATH, HttpMethod.POST, body, new HashMap<>(), retryPolicy);
    }

    private HttpRetryPolicy.ResponseWithBody requestHelper(
            String path,
            HttpMethod method,
            RequestBody body,
            Map<String, String> headers,
            HttpRetryPolicy retryPolicy) throws SQLException {
        SessionState currentSession = this.session.get();
        HttpUrl url = buildUrl(path);

        Request.Builder builder = new Request.Builder().url(url);
        this.newAdditionalHeaders().forEach(builder::addHeader);
        if (headers != null) {
            headers.forEach(builder::addHeader);
        }
        if (currentSession != null && currentSession.getNeedSticky()) {
            builder.addHeader(QueryRequestConfig.X_DATABEND_ROUTE_HINT, url.host());
            String stickyNode = this.lastNodeID.get();
            if (stickyNode != null) {
                builder.addHeader(QueryRequestConfig.X_DATABEND_STICKY_NODE, stickyNode);
            }
        }

        switch (method) {
            case POST:
                builder = builder.post(body);
                break;
            case PUT:
                builder = builder.put(body);
                break;
            case GET:
            default:
                builder = builder.get();
                break;
        }
        return retryPolicy.sendRequestWithRetry(this.httpClient, builder.build());
    }

    private HttpUrl buildUrl(String path) {
        HttpUrl url = HttpUrl.get(this.config.getBaseUri().toString());
        return url.newBuilder().encodedPath(path).build();
    }

    private void executeStageUpload(Request request) throws IOException {
        requireNonNull(request, "request is null");

        OkHttpClient uploadClient = httpClient.newBuilder()
                .connectTimeout(600, java.util.concurrent.TimeUnit.SECONDS)
                .writeTimeout(900, java.util.concurrent.TimeUnit.SECONDS)
                .readTimeout(600, java.util.concurrent.TimeUnit.SECONDS)
                .retryOnConnectionFailure(true)
                .protocols(Arrays.asList(Protocol.HTTP_1_1))
                .build();

        long start = System.nanoTime();
        long attempts = 0;
        Exception cause = null;
        while (true) {
            if (attempts > 0 && request.body() != null && request.body().isOneShot()) {
                logger.warning("Upload to stage retry aborted because request body is not replayable");
                throw replayAbortedStageUploadException(cause);
            }
            if (attempts > 0) {
                logger.info("try to upload to stage again: " + attempts + ", cause: " + cause);
                Duration sinceStart = Duration.ofNanos(System.nanoTime() - start);
                if (sinceStart.compareTo(STAGE_UPLOAD_RETRY_TIMEOUT) >= 0 || attempts >= MAX_STAGE_UPLOAD_RETRY_ATTEMPTS) {
                    logger.warning("Upload to stage failed, error is: " + cause);
                    throw new DatabendStageUploadException(
                            String.format("Error upload to stage (attempts: %s, duration: %s)", attempts, sinceStart),
                            cause);
                }

                try {
                    MILLISECONDS.sleep(attempts * 100);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("StatementClient thread was interrupted");
                }
            }
            attempts++;

            Response response = null;
            try {
                response = uploadClient.newCall(request).execute();
                if (response.isSuccessful()) {
                    return;
                }
                if (response.code() == 401) {
                    throw new NonRetryableStageUploadFailure("Error upload to stage, Unauthorized user: "
                            + response.code() + " " + response.message());
                }
                if (response.code() >= 503) {
                    throw new RetryableHttpStatusException("Error upload to stage, service unavailable: "
                            + response.code() + " " + response.message());
                }
                else if (response.code() >= 400) {
                    throw new NonRetryableStageUploadFailure("Error upload to stage, configuration error: "
                            + response.code() + " " + response.message());
                }
            }
            catch (IOException e) {
                if (!HttpRetryPolicy.isRetryableIOException(e)) {
                    throw e;
                }
                cause = e;
            }
            catch (NonRetryableStageUploadFailure e) {
                throw new DatabendStageUploadException(e.getMessage(), e);
            }
            finally {
                if (response != null) {
                    try {
                        response.close();
                    }
                    catch (Exception ignored) {
                    }
                }
            }
        }
    }

    private MultipartBody buildMultipartBody(InputStream inputStream, long fileSize) {
        RequestBody requestBody = new RequestBody() {
            @Override
            public MediaType contentType() {
                return MediaType.parse("application/octet-stream");
            }

            @Override
            public long contentLength() {
                return fileSize;
            }

            @Override
            public void writeTo(BufferedSink sink) throws IOException {
                try (Source source = Okio.source(inputStream)) {
                    sink.writeAll(source);
                } catch (IOException e) {
                    throw new StreamingLoadRequestEncodingFailure("Failed to encode streaming load request body", e);
                }
            }
        };
        return new MultipartBody.Builder()
                .setType(MultipartBody.FORM)
                .addFormDataPart("upload", "java.io.InputStream", requestBody)
                .build();
    }

    private void sendHeartbeat(List<QueryLiveness> queryLivenesses) {
        long now = System.currentTimeMillis();
        Map<String, ArrayList<String>> nodeToQueryID = new HashMap<>();
        Map<String, QueryLiveness> queries = new HashMap<>();

        for (QueryLiveness ql : queryLivenesses) {
            if (now - ql.lastRequestTime.get() >= ql.resultTimeoutSecs * 1000 / 2) {
                nodeToQueryID.computeIfAbsent(ql.nodeID, key -> new ArrayList<>()).add(ql.queryID);
                queries.put(ql.queryID, ql);
            }
        }
        if (nodeToQueryID.isEmpty()) {
            return;
        }

        Map<String, Object> requestBody = new HashMap<>();
        requestBody.put("node_to_queries", nodeToQueryID);

        String json;
        try {
            json = objectMapper.writeValueAsString(requestBody);
        } catch (JsonProcessingException e) {
            logger.warning("fail to encode heartbeat body: " + e);
            return;
        }

        HttpRetryPolicy.ResponseWithBody response;
        try {
            HttpRetryPolicy retryPolicy = new HttpRetryPolicy(true, false);
            response = requestHelper(
                    HEARTBEAT_PATH,
                    HttpMethod.POST,
                    RequestBody.create(MEDIA_TYPE_JSON, json),
                    null,
                    retryPolicy);
        } catch (SQLException e) {
            logger.warning("fail to send heartbeat: " + e);
            return;
        }

        try {
            JsonNode toRemove = objectMapper.readTree(response.body).get("queries_to_remove");
            if (toRemove != null && !toRemove.isNull() && !toRemove.isArray()) {
                throw new IllegalArgumentException("queries_to_remove is not an array");
            }
            if (toRemove != null) {
                for (JsonNode element : toRemove) {
                    QueryLiveness query = queries.get(element.asText());
                    if (query != null) {
                        query.stopped = true;
                    }
                }
            }
        } catch (IOException | IllegalArgumentException e) {
            logger.warning("fail to decode heartbeat response: " + e);
        }
    }

    private PresignedRequestContext getPresignedRequest(PresignMethod method, String stageName, String fileName)
            throws SQLException {
        String sql = buildPresignSql(method, stageName, fileName);
        QueryResultPages pages = startQuery(UUID.randomUUID().toString().replace("-", ""), sql, null, QueryResultFormat.JSON);
        try {
            while (pages.hasNext()) {
                QueryResults results = pages.getResults();
                if (results != null && results.getError() != null) {
                    throw new SQLException("Failed to get presign url: " + results.getError());
                }
                List<List<Object>> rows = results.getData();
                if (rows != null && !rows.isEmpty()) {
                    try {
                        int headersIndex = findFieldIndex(results, "headers");
                        int urlIndex = findFieldIndex(results, "url");
                        List<Object> row = rows.get(0);
                        if (headersIndex >= row.size() || urlIndex >= row.size()) {
                            throw new DatabendPresignException("Invalid presign response row for SQL: " + sql);
                        }
                        String headers = String.valueOf(row.get(headersIndex));
                        String url = String.valueOf(row.get(urlIndex));
                        return new PresignedRequestContext(parseHeaders(headers), url);
                    } catch (JsonProcessingException | IllegalArgumentException e) {
                        throw new DatabendPresignException("Failed to decode presign response for SQL: " + sql, e);
                    } catch (SQLException e) {
                        throw new DatabendPresignException("Failed to decode presign response for SQL: " + sql, e);
                    }
                }
                pages.advance();
            }
            throw new DatabendPresignException("Failed to decode presign response for SQL: " + sql + ": no result returned");
        } finally {
            pages.close();
        }
    }

    private static int findFieldIndex(QueryResults results, String fieldName) throws SQLException {
        for (int i = 0; i < results.getSchema().size(); i++) {
            if (fieldName.equalsIgnoreCase(results.getSchema().get(i).getName())) {
                return i;
            }
        }
        throw new SQLException("Presign response does not contain field: " + fieldName);
    }

    private static Headers parseHeaders(String headersJson) throws JsonProcessingException {
        Map<String, Object> response = objectMapper.readValue(headersJson, HashMap.class);
        Headers.Builder builder = new Headers.Builder();
        for (Map.Entry<String, Object> entry : response.entrySet()) {
            builder.add(entry.getKey(), entry.getValue().toString());
        }
        return builder.build();
    }

    private static String buildPresignSql(PresignMethod method, String stageName, String fileName) {
        StringBuilder sql = new StringBuilder("PRESIGN ");
        sql.append(method == PresignMethod.UPLOAD ? "UPLOAD" : "DOWNLOAD");
        sql.append(" @");
        if (stageName != null) {
            sql.append(stageName).append("/");
        } else {
            sql.append("~/");
        }
        sql.append(fileName);
        return sql.toString();
    }

    private static String uriRouteHint(String uri) {
        return Base64.getEncoder().encodeToString(uri.getBytes()) + "#";
    }

    private static ScheduledExecutorService getScheduler() {
        if (heartbeatScheduler == null) {
            synchronized (DatabendSessionHandle.class) {
                if (heartbeatScheduler == null) {
                    heartbeatScheduler = Executors.newScheduledThreadPool(
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

    private enum HttpMethod {
        GET,
        POST,
        PUT
    }

    private enum PresignMethod {
        UPLOAD,
        DOWNLOAD
    }

    private static final class LoginRequest {
        public String database;
        public Map<String, String> settings;
    }

    private static final class PresignedRequestContext {
        private final Headers headers;
        private final String url;

        private PresignedRequestContext(Headers headers, String url) {
            this.headers = headers;
            this.url = url;
        }
    }

    private static final class NonRetryableStageUploadFailure extends RuntimeException {
        private NonRetryableStageUploadFailure(String message) {
            super(message);
        }
    }

    private static DatabendStageUploadException replayAbortedStageUploadException(Exception cause) {
        String message = cause != null && cause.getMessage() != null
                ? cause.getMessage()
                : "Upload to stage retry aborted because request body is not replayable";
        return new DatabendStageUploadException(message, cause);
    }

    private static final class StreamingLoadRequestEncodingFailure extends IOException {
        private StreamingLoadRequestEncodingFailure(String message, Throwable cause) {
            super(message, cause);
        }
    }

    private static final class StageUploadRequestBody extends RequestBody {
        private final InputStream inputStream;
        private final long fileSize;

        private StageUploadRequestBody(InputStream inputStream, long fileSize) {
            this.inputStream = requireNonNull(inputStream, "inputStream is null");
            this.fileSize = fileSize;
        }

        @Override
        public MediaType contentType() {
            return null;
        }

        @Override
        public long contentLength() {
            return fileSize;
        }

        @Override
        public boolean isOneShot() {
            return true;
        }

        @Override
        public void writeTo(BufferedSink sink) throws IOException {
            Source source = Okio.source(inputStream);
            sink.writeAll(source);
        }
    }

    private final class HeartbeatManager implements Runnable {
        private ScheduledFuture<?> heartbeatFuture;
        private long heartbeatIntervalMillis = 30000;
        private long lastHeartbeatStartTimeMillis = 0;
        private boolean closed;

        private synchronized void onStartQuery(Long timeoutSecs) {
            if (closed || !supportsHeartBeat()) {
                return;
            }
            long candidateInterval = timeoutSecs * 1000 / 4;
            if (candidateInterval < heartbeatIntervalMillis) {
                heartbeatIntervalMillis = candidateInterval;
                if (heartbeatFuture != null) {
                    heartbeatFuture.cancel(false);
                    heartbeatFuture = null;
                }
            }
            if (heartbeatFuture == null) {
                scheduleHeartbeat();
            }
        }

        private synchronized void stop() {
            closed = true;
            if (heartbeatFuture != null) {
                heartbeatFuture.cancel(false);
                heartbeatFuture = null;
            }
        }

        private synchronized boolean isStopped() {
            return heartbeatFuture == null;
        }

        private synchronized void scheduleHeartbeat() {
            long delay = Math.max(heartbeatIntervalMillis - (System.currentTimeMillis() - lastHeartbeatStartTimeMillis), 0);
            heartbeatFuture = getScheduler().schedule(this, delay, MILLISECONDS);
        }

        @Override
        public void run() {
            lastHeartbeatStartTimeMillis = System.currentTimeMillis();
            List<QueryLiveness> queryLivenesses = queryLivenessSupplier.get();
            List<QueryLiveness> activeQueries = new ArrayList<>();
            for (QueryLiveness queryLiveness : queryLivenesses) {
                if (queryLiveness != null && !queryLiveness.stopped && queryLiveness.serverSupportHeartBeat) {
                    activeQueries.add(queryLiveness);
                }
            }

            sendHeartbeat(activeQueries);

            synchronized (this) {
                heartbeatFuture = null;
                if (!closed && !activeQueries.isEmpty()) {
                    scheduleHeartbeat();
                }
            }
        }
    }
}
