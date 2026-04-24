package com.databend.jdbc.internal.query;

import com.databend.jdbc.internal.error.QueryError;
import com.databend.jdbc.internal.exception.DatabendQueryException;
import com.databend.jdbc.internal.http.JsonCodec;
import com.databend.jdbc.internal.http.JsonResponse;
import com.databend.jdbc.internal.http.HttpRetryPolicy;
import com.databend.jdbc.internal.session.QueryRequestConfig;
import com.databend.jdbc.internal.session.SessionState;
import okhttp3.Headers;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;

import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;
import java.net.URI;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static com.databend.jdbc.internal.http.JsonCodec.jsonCodec;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class RestQueryResultPages implements QueryResultPages {
    public static final String USER_AGENT_VALUE = RestQueryResultPages.class.getSimpleName() + "/" + "jvm-unknown";
    public static final MediaType MEDIA_TYPE_JSON = MediaType.parse("application/json; charset=utf-8");
    public static final JsonCodec<QueryResults> QUERY_RESULTS_CODEC = jsonCodec(QueryResults.class);
    public static final String QUERY_PATH = "/v1/query";

    private final AtomicReference<Boolean> finished = new AtomicReference<>(false);
    private final OkHttpClient httpClient;
    private final String query;
    private final String host;
    private final Map<String, String> additionalHeaders;
    private final AtomicReference<SessionState> databendSession;
    private final AtomicReference<QueryResults> currentResults = new AtomicReference<>(null);
    private final Consumer<SessionState> onSessionStateUpdate;
    private String nodeID;

    public RestQueryResultPages(OkHttpClient httpClient, String sql, QueryRequestConfig settings, Consumer<SessionState> onSessionStateUpdate, AtomicReference<String> lastNodeID) {
        requireNonNull(httpClient, "httpClient is null");
        requireNonNull(sql, "sql is null");
        requireNonNull(settings, "settings is null");
        requireNonNull(settings.getHost(), "settings.host is null");
        this.httpClient = httpClient;
        this.query = sql;
        this.onSessionStateUpdate = onSessionStateUpdate;
        this.host = settings.getHost();
        this.additionalHeaders = settings.getAdditionalHeaders();
        this.databendSession = new AtomicReference<>(settings.getSession());
        this.nodeID = lastNodeID.get();

        Request request = buildQueryRequest(query, settings);
        boolean completed = executeInternal(request);
        if (!completed) {
            throw new DatabendQueryException("Query failed to complete");
        }
        lastNodeID.set(this.nodeID);
    }

    public static Request.Builder prepareRequest(HttpUrl url, Map<String, String> additionalHeaders) {
        Request.Builder builder = new Request.Builder()
                .url(url)
                .header("User-Agent", USER_AGENT_VALUE)
                .header("Accept", "application/json")
                .header("Content-Type", "application/json");
        if (additionalHeaders != null) {
            additionalHeaders.forEach(builder::addHeader);
        }
        return builder;
    }

    private Request buildQueryRequest(String query, QueryRequestConfig settings) {
        HttpUrl url = HttpUrl.parse(settings.getHost());
        if (url == null) {
            throw new IllegalArgumentException("Invalid host: " + settings.getHost());
        }
        QueryRequest req = QueryRequest.builder()
                .setSession(settings.getSession())
                .setStageAttachment(settings.getStageAttachment())
                .setPaginationOptions(settings.getPaginationOptions())
                .setSql(query)
                .build();
        String reqString = req.toString();
        if (reqString == null || reqString.isEmpty()) {
            throw new IllegalArgumentException("Invalid request: " + req);
        }
        url = url.newBuilder().encodedPath(QUERY_PATH).build();
        Request.Builder builder = prepareRequest(url, this.additionalHeaders);
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
            JsonResponse<QueryResults> response = JsonResponse.decode(QUERY_RESULTS_CODEC, resp);
            if (response.getStatusCode() == HTTP_OK && response.hasValue()) {
                QueryError error = response.getValue().getError();
                if (error == null) {
                    processResponse(response.getHeaders(), response.getValue());
                    return true;
                }
                throw new DatabendQueryException("Query Failed: " + error);
            }
            return false;
        } catch (SQLException e) {
            throw new DatabendQueryException("Failed to execute query request", e);
        }
    }

    private void processResponse(Headers headers, QueryResults results) {
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
            closeQuery();
            return false;
        }

        String nextUriPath = this.currentResults.get().getNextUri().toString();
        HttpUrl url = HttpUrl.get(this.host);
        url = url.newBuilder().encodedPath(nextUriPath).build();
        Request.Builder builder = prepareRequest(url, this.additionalHeaders);
        builder.addHeader(QueryRequestConfig.X_DATABEND_STICKY_NODE, this.nodeID);
        Request request = builder.get().build();
        return executeInternal(request);
    }

    @Override
    public boolean hasNext() {
        return !finished.get();
    }

    @Override
    public Map<String, String> getAdditionalHeaders() {
        return additionalHeaders;
    }

    @Override
    public QueryResults getResults() {
        return currentResults.get();
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
        Request request = prepareRequest(url, this.additionalHeaders).get().build();
        try {
            httpClient.newCall(request).execute().close();
        } catch (IOException ignored) {
        }
    }
}
