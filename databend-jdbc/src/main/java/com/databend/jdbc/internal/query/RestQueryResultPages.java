package com.databend.jdbc.internal.query;

import com.databend.jdbc.internal.QueryResultFormat;
import com.databend.jdbc.internal.error.QueryError;
import com.databend.jdbc.internal.exception.DatabendQueryException;
import com.databend.jdbc.internal.http.HttpRetryPolicy;
import com.databend.jdbc.internal.http.JsonCodec;
import com.databend.jdbc.internal.http.JsonResponse;
import com.databend.jdbc.internal.session.QueryRequestConfig;
import com.databend.jdbc.internal.session.SessionState;
import okhttp3.Headers;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import org.apache.arrow.compression.CommonsCompressionFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;

import javax.annotation.concurrent.ThreadSafe;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static com.databend.jdbc.internal.http.JsonCodec.jsonCodec;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class RestQueryResultPages implements QueryResultPages {
    private static final int ARROW_FEATURE_NEGOTIATION_VERSION = 3;
    public static final MediaType MEDIA_TYPE_JSON = MediaType.parse("application/json; charset=utf-8");
    public static final MediaType MEDIA_TYPE_ARROW = MediaType.parse("application/vnd.apache.arrow.stream");
    public static final JsonCodec<QueryResults> QUERY_RESULTS_CODEC = jsonCodec(QueryResults.class);
    public static final String QUERY_PATH = "/v1/query";

    private final AtomicReference<Boolean> finished = new AtomicReference<>(false);
    private final OkHttpClient httpClient;
    private final String query;
    private final String host;
    private final QueryRequestConfig requestConfig;
    private final AtomicReference<QueryResultFormat> queryResultFormat;
    private final Map<String, String> additionalHeaders;
    private final AtomicReference<SessionState> databendSession;
    private final AtomicReference<QueryResults> currentResults = new AtomicReference<>(null);
    private final AtomicReference<List<QueryRowField>> currentSchema = new AtomicReference<>(null);
    private final AtomicReference<ResultPage> currentPage = new AtomicReference<>(new JsonResultPage(null));
    private final Consumer<SessionState> onSessionStateUpdate;
    private String nodeID;

    public RestQueryResultPages(OkHttpClient httpClient, String sql, QueryRequestConfig requestConfig, Consumer<SessionState> onSessionStateUpdate, AtomicReference<String> lastNodeID) {
        requireNonNull(httpClient, "httpClient is null");
        requireNonNull(sql, "sql is null");
        requireNonNull(requestConfig, "requestConfig is null");
        requireNonNull(requestConfig.getHost(), "requestConfig.host is null");
        this.httpClient = httpClient;
        this.query = sql;
        this.onSessionStateUpdate = onSessionStateUpdate;
        this.host = requestConfig.getHost();
        this.requestConfig = requestConfig;
        this.queryResultFormat = new AtomicReference<>(requestConfig.getQueryResultFormat());
        this.additionalHeaders = requestConfig.getAdditionalHeaders();
        this.databendSession = new AtomicReference<>(requestConfig.getSession());
        this.nodeID = lastNodeID.get();

        Request request = buildQueryRequest(query, requestConfig);
        boolean completed = executeInternal(request);
        if (!completed) {
            throw new DatabendQueryException("Query failed to complete");
        }
        lastNodeID.set(this.nodeID);
    }

    public static Request.Builder prepareRequest(HttpUrl url, Map<String, String> additionalHeaders, QueryResultFormat queryResultFormat) {
        Request.Builder builder = new Request.Builder()
                .url(url)
                .header("Accept", queryResultFormat == QueryResultFormat.ARROW ? MEDIA_TYPE_ARROW.toString() : "application/json")
                .header("Content-Type", "application/json");
        if (additionalHeaders != null) {
            additionalHeaders.forEach(builder::addHeader);
        }
        return builder;
    }

    private Request buildQueryRequest(String query, QueryRequestConfig requestConfig) {
        HttpUrl url = HttpUrl.parse(requestConfig.getHost());
        if (url == null) {
            throw new IllegalArgumentException("Invalid host: " + requestConfig.getHost());
        }
        QueryResultFormat currentFormat = queryResultFormat.get();
        QueryRequest req = QueryRequest.builder()
                .setSession(requestConfig.getSession())
                .setStageAttachment(requestConfig.getStageAttachment())
                .setPaginationOptions(requestConfig.getPaginationOptions())
                .setSql(query)
                .setArrowResultVersionMax(currentFormat == QueryResultFormat.ARROW ? ARROW_FEATURE_NEGOTIATION_VERSION : null)
                .setArrowFeatures(currentFormat == QueryResultFormat.ARROW ? new QueryRequest.ArrowFeatures(false) : null)
                .build();
        String reqString = req.toString();
        if (reqString == null || reqString.isEmpty()) {
            throw new IllegalArgumentException("Invalid request: " + req);
        }
        url = url.newBuilder().encodedPath(QUERY_PATH).build();
        Request.Builder builder = prepareRequest(url, this.additionalHeaders, currentFormat);
        SessionState session = databendSession.get();
        if (session != null && session.getNeedSticky()) {
            builder.addHeader(QueryRequestConfig.X_DATABEND_STICKY_NODE, nodeID);
        }
        return builder.post(RequestBody.create(MEDIA_TYPE_JSON, reqString)).build();
    }

    @Override
    public String getQuery() {
        return query;
    }

    @Override
    public boolean execute(Request request) {
        return executeInternal(request);
    }

    private boolean executeInternal(Request request) {
        requireNonNull(request, "request is null");
        try {
            HttpRetryPolicy retryPolicy = new HttpRetryPolicy(false, true);
            HttpRetryPolicy.ResponseWithBody resp = retryPolicy.sendRequestWithRetry(httpClient, request);
            ResponsePayload payload = decodeResponse(resp);
            if (payload.statusCode == HTTP_OK && payload.results != null) {
                QueryError error = payload.results.getError();
                if (error == null) {
                    processResponse(payload.headers, payload.results, payload.page, payload.schema);
                    return true;
                }
                throw new DatabendQueryException("Query Failed: " + error);
            }
            return false;
        } catch (SQLException e) {
            throw new DatabendQueryException("Failed to execute query request", e);
        }
    }

    private ResponsePayload decodeResponse(HttpRetryPolicy.ResponseWithBody responseWithBody) throws SQLException {
        if (isArrow(responseWithBody.contentType)) {
            return decodeArrowResponse(responseWithBody);
        }

        JsonResponse<QueryResults> response = JsonResponse.decode(QUERY_RESULTS_CODEC, responseWithBody);
        QueryResults results = response.hasValue() ? response.getValue() : null;
        return new ResponsePayload(
                response.getStatusCode(),
                response.getHeaders(),
                results,
                new JsonResultPage(results == null ? null : results.getData()),
                results == null ? null : results.getSchema());
    }

    private ResponsePayload decodeArrowResponse(HttpRetryPolicy.ResponseWithBody responseWithBody) throws SQLException {
        BufferAllocator allocator = rootAllocator().newChildAllocator("databend-jdbc-arrow-page", 0, Long.MAX_VALUE);
        try (ArrowStreamReader reader = new ArrowStreamReader(
                new ByteArrayInputStream(responseWithBody.body),
                allocator,
                CommonsCompressionFactory.INSTANCE)) {
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            org.apache.arrow.vector.types.pojo.Schema schema = root.getSchema();
            String responseHeader = schema.getCustomMetadata().get("response_header");
            if (responseHeader == null) {
                throw new DatabendQueryException("Missing response_header metadata in Arrow payload");
            }

            QueryResults results = QUERY_RESULTS_CODEC.fromJson(responseHeader);
            List<ArrowRecordBatch> recordBatches = new ArrayList<>();
            while (reader.loadNextBatch()) {
                recordBatches.add(new VectorUnloader(root).getRecordBatch());
            }

            ResultPage page = ArrowResultPage.fromRecordBatches(allocator, schema, recordBatches, effectiveSettings(results));
            return new ResponsePayload(
                    responseWithBody.statusCode,
                    responseWithBody.headers,
                    results,
                    page,
                    ArrowResultPage.schemaToFields(schema));
        } catch (Exception e) {
            allocator.close();
            if (e instanceof SQLException) {
                throw (SQLException) e;
            }
            throw new SQLException("Failed to decode Arrow response", e);
        }
    }

    private void processResponse(Headers headers, QueryResults results, ResultPage page, List<QueryRowField> schema) {
        nodeID = results.getNodeId();
        SessionState session = results.getSession();
        if (session != null) {
            databendSession.set(session);
            if (this.onSessionStateUpdate != null) {
                this.onSessionStateUpdate.accept(session);
            }
        }
        if (results.getQueryId() != null && this.additionalHeaders.get(QueryRequestConfig.X_DATABEND_QUERY_ID) == null) {
            this.additionalHeaders.put(QueryRequestConfig.X_DATABEND_QUERY_ID, results.getQueryId());
        }
        if (headers != null) {
            String routeHint = headers.get(QueryRequestConfig.X_DATABEND_ROUTE_HINT);
            if (routeHint != null) {
                this.additionalHeaders.put(QueryRequestConfig.X_DATABEND_ROUTE_HINT, routeHint);
            }
        }
        currentPage.set(page);
        currentSchema.set(schema);
        currentResults.set(results);
    }

    @Override
    public boolean advance() {
        requireNonNull(this.host, "host is null");
        requireNonNull(this.currentResults.get(), "currentResults is null");
        if (finished.get()) {
            return false;
        }
        if (!this.currentResults.get().hasMoreData()) {
            currentPage.set(null);
            finished.set(true);
            return false;
        }

        String nextUriPath = this.currentResults.get().getNextUri().toString();
        HttpUrl url = HttpUrl.get(this.host);
        url = url.newBuilder().encodedPath(nextUriPath).build();
        Request.Builder builder = prepareRequest(url, this.additionalHeaders, this.queryResultFormat.get());
        builder.addHeader(QueryRequestConfig.X_DATABEND_STICKY_NODE, this.nodeID);
        Request request = builder.get().build();
        return executeInternal(request);
    }

    @Override
    public boolean hasNext() {
        return !finished.get();
    }

    @Override
    public QueryResults getResults() {
        return currentResults.get();
    }

    @Override
    public List<QueryRowField> getSchema() {
        return currentSchema.get();
    }

    @Override
    public ResultPage getPage() {
        return currentPage.get();
    }

    @Override
    public SessionState getSession() {
        return databendSession.get();
    }

    @Override
    public String getNodeID() {
        return this.nodeID;
    }

    @Override
    public void close() {
        closeQuery();
    }

    private void closeQuery() {
        if (!finished.compareAndSet(false, true)) {
            return;
        }

        ResultPage page = currentPage.getAndSet(null);
        if (page != null) {
            page.close();
        }

        QueryResults q = this.currentResults.get();
        if (q == null) {
            return;
        }
        URI uri = q.getFinalUri();
        if (uri == null) {
            return;
        }
        String path = uri.toString();
        HttpUrl url = HttpUrl.get(this.host);
        url = url.newBuilder().encodedPath(path).build();
        Request request = prepareRequest(url, this.additionalHeaders, QueryResultFormat.JSON).get().build();
        try {
            httpClient.newCall(request).execute().close();
        } catch (IOException ignored) {
        }
    }

    private static boolean isArrow(MediaType mediaType) {
        return mediaType != null
                && "application".equalsIgnoreCase(mediaType.type())
                && "vnd.apache.arrow.stream".equalsIgnoreCase(mediaType.subtype());
    }

    private static RootAllocator rootAllocator() {
        return RootAllocatorHolder.INSTANCE;
    }

    private static Map<String, String> effectiveSettings(QueryResults results) {
        Map<String, String> merged = new HashMap<>();
        if (results.getSession() != null && results.getSession().getSettings() != null) {
            merged.putAll(results.getSession().getSettings());
        }
        if (results.getSettings() != null) {
            merged.putAll(results.getSettings());
        }
        return merged;
    }

    private static final class ResponsePayload {
        private final int statusCode;
        private final Headers headers;
        private final QueryResults results;
        private final ResultPage page;
        private final List<QueryRowField> schema;

        private ResponsePayload(int statusCode, Headers headers, QueryResults results, ResultPage page, List<QueryRowField> schema) {
            this.statusCode = statusCode;
            this.headers = headers;
            this.results = results;
            this.page = page;
            this.schema = schema;
        }
    }

    private static final class RootAllocatorHolder {
        private static final RootAllocator INSTANCE = new RootAllocator(Long.MAX_VALUE);
    }
}
