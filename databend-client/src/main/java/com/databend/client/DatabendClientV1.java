/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.databend.client;

import com.databend.client.errors.QueryErrors;
import okhttp3.*;

import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;
import java.net.URI;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static com.databend.client.JsonCodec.jsonCodec;
import static com.databend.client.constant.DatabendConstant.BOOLEAN_TRUE_STR;
import static com.google.common.base.MoreObjects.firstNonNull;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class DatabendClientV1
        implements DatabendClient {
    private final AtomicReference<Boolean> finished = new AtomicReference<>(false);
    public static final String USER_AGENT_VALUE = DatabendClientV1.class.getSimpleName() +
            "/" +
            firstNonNull(DatabendClientV1.class.getPackage().getImplementationVersion(), "jvm-unknown");
    public static final MediaType MEDIA_TYPE_JSON = MediaType.parse("application/json; charset=utf-8");
    public static final JsonCodec<QueryResults> QUERY_RESULTS_CODEC = jsonCodec(QueryResults.class);
    public static final JsonCodec<DiscoveryResponseCodec.DiscoveryResponse> DISCOVERY_RESULT_CODEC = jsonCodec(DiscoveryResponseCodec.DiscoveryResponse.class);

    public static final String QUERY_PATH = "/v1/query";
    public static final String DISCOVERY_PATH = "/v1/discovery_nodes";
    private final OkHttpClient httpClient;
    private final String query;
    private final String host;

    private final Map<String, String> additionalHeaders;
    // client session
    private final AtomicReference<DatabendSession> databendSession;
    private String nodeID;
    private final AtomicReference<QueryResults> currentResults = new AtomicReference<>(null);
    private final Consumer<DatabendSession> on_session_state_update;

    public DatabendClientV1(OkHttpClient httpClient, String sql, ClientSettings settings,
                            Consumer<DatabendSession> on_session_state_update,
                            AtomicReference<String> last_node_id) {
        requireNonNull(httpClient, "httpClient is null");
        requireNonNull(sql, "sql is null");
        requireNonNull(settings, "settings is null");
        requireNonNull(settings.getHost(), "settings.host is null");
        this.httpClient = httpClient;
        this.query = sql;
        this.on_session_state_update = on_session_state_update;
        this.host = settings.getHost();
        this.additionalHeaders = settings.getAdditionalHeaders();
        this.databendSession = new AtomicReference<>(settings.getSession());
        this.nodeID = last_node_id.get();

        Request request = buildQueryRequest(query, settings);
        boolean completed = this.execute(request);

        if (!completed) {
            throw new RuntimeException("Query failed to complete");
        }

        last_node_id.set(this.nodeID);
    }

    public static List<DiscoveryNode> discoverNodes(OkHttpClient httpClient, ClientSettings settings) {
        requireNonNull(httpClient, "httpClient is null");
        requireNonNull(settings, "settings is null");
        requireNonNull(settings.getHost(), "settings.host is null");
        Request request = buildDiscoveryRequest(settings);
        DiscoveryResponseCodec.DiscoveryResponse response = getDiscoveryResponse(httpClient, request);
        return response.getNodes();
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

    private Request buildQueryRequest(String query, ClientSettings settings) {
        HttpUrl url = HttpUrl.parse(settings.getHost());
        if (url == null) {
            // TODO(zhihanz) use custom exception
            throw new IllegalArgumentException("Invalid host: " + settings.getHost());
        }

        QueryRequest req = QueryRequest.builder().setSession(settings.getSession()).setStageAttachment(settings.getStageAttachment()).setPaginationOptions(settings.getPaginationOptions()).setSql(query).build();
        String reqString = req.toString();
        if (reqString == null || reqString.isEmpty()) {
            throw new IllegalArgumentException("Invalid request: " + req);
        }

        url = url.newBuilder().encodedPath(QUERY_PATH).build();
        Request.Builder builder = prepareRequest(url, this.additionalHeaders);
        DatabendSession session = databendSession.get();
        if (session != null && session.getNeedSticky()) {
            builder.addHeader(ClientSettings.X_DATABEND_STICKY_NODE, nodeID);
        }
        return builder.post(okhttp3.RequestBody.create(MEDIA_TYPE_JSON, reqString)).build();
    }

    private static Request buildDiscoveryRequest(ClientSettings settings) {
        HttpUrl url = HttpUrl.get(settings.getHost());
        String discoveryPath = DISCOVERY_PATH;
        // intentionally use unsupported discovery path for testing
        if (settings.getAdditionalHeaders().get("~mock.unsupported.discovery") != null && BOOLEAN_TRUE_STR.equals(settings.getAdditionalHeaders().get("~mock.unsupported.discovery"))) {
            discoveryPath = "/v1/discovery_nodes_unsupported";
        }

        url = url.newBuilder().encodedPath(discoveryPath).build();
        Request.Builder builder = prepareRequest(url, settings.getAdditionalHeaders());
        return builder.get().build();
    }

    @Override
    public String getQuery() {
        return query;
    }

    private static DiscoveryResponseCodec.DiscoveryResponse getDiscoveryResponse(OkHttpClient httpClient, Request request) {
        requireNonNull(request, "request is null");
        JsonResponse<DiscoveryResponseCodec.DiscoveryResponse> response;

        try {
            RetryPolicy retryPolicy = new RetryPolicy(true, true);
            RetryPolicy.ResponseWithBody resp = retryPolicy.sendRequestWithRetry(httpClient, request);
            long code = resp.response.code();
            if (code == HTTP_OK) {
                response = JsonResponse.decode(DISCOVERY_RESULT_CODEC, resp);
                DiscoveryResponseCodec.DiscoveryResponse discoveryResponse = response.getValue();
                QueryErrors  errors = discoveryResponse.getError();
                if (errors  == null) {
                    return discoveryResponse;
                } else {
                    throw new RuntimeException("Discovery request failed: " + discoveryResponse.getError());
                }
            } else if (code == HTTP_NOT_FOUND) {
                throw new UnsupportedOperationException("Discovery request feature not supported");
            }
            throw new RuntimeException("Discovery request failed, code = " + code + " :" + resp.body);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean executeInternal(Request request) {
        requireNonNull(request, "request is null");
        JsonResponse<QueryResults> response;

        try {
            RetryPolicy retryPolicy = new RetryPolicy(false, true);
            RetryPolicy.ResponseWithBody resp = retryPolicy.sendRequestWithRetry(httpClient, request);
            response = JsonResponse.decode(QUERY_RESULTS_CODEC, resp);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        if ((response.getStatusCode() == HTTP_OK) && response.hasValue() ) {
            QueryErrors e = response.getValue().getError();
            if (e == null) {
                processResponse(response.getHeaders(), response.getValue());
            } else {
                throw new RuntimeException("Query Failed: " + e);
            }
            return true;
        }
        return false;
    }

    @Override
    public boolean execute(Request request) {
        return executeInternal(request);
    }

    private void processResponse(Headers headers, QueryResults results) {
        nodeID = results.getNodeId();
        DatabendSession session = results.getSession();
        if (session != null) {
            databendSession.set(session);
            if (this.on_session_state_update != null) {
                this.on_session_state_update.accept(session);
            }
        }
        if (results.getQueryId() != null && this.additionalHeaders.get(ClientSettings.X_Databend_Query_ID) == null) {
            this.additionalHeaders.put(ClientSettings.X_Databend_Query_ID, results.getQueryId());
        }
        if (headers != null) {
            String route_hint = headers.get(ClientSettings.X_DATABEND_ROUTE_HINT);
            if (route_hint != null) {
                this.additionalHeaders.put(ClientSettings.X_DATABEND_ROUTE_HINT, route_hint);
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
            // no need to fetch next page
            return false;
        }

        String nextUriPath = this.currentResults.get().getNextUri().toString();
        HttpUrl url = HttpUrl.get(this.host);
        url = url.newBuilder().encodedPath(nextUriPath).build();
        Request.Builder builder = prepareRequest(url, this.additionalHeaders);
        builder.addHeader(ClientSettings.X_DATABEND_STICKY_NODE, this.nodeID);
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
    public DatabendSession getSession() {
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
        Request r = prepareRequest(url, this.additionalHeaders).get().build();
        try {
            httpClient.newCall(r).execute().close();
        } catch (IOException ignored) {
        }
    }
}
