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

import com.databend.client.errors.CloudErrors;
import okhttp3.*;
import okio.Buffer;

import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;
import java.net.ConnectException;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.logging.Logger;

import static com.databend.client.JsonCodec.jsonCodec;
import static com.google.common.base.MoreObjects.firstNonNull;
import static java.lang.String.format;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

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
    public static final String succeededState = "succeeded";
    public static final String failedState = "failed";
    public static final String runningState = "running";


    public static final String QUERY_PATH = "/v1/query";
    public static final String DISCOVERY_PATH = "/v1/discovery_nodes";
    private static final long MAX_MATERIALIZED_JSON_RESPONSE_SIZE = 128 * 1024;
    private final OkHttpClient httpClient;
    private final String query;
    private final String host;

    private final int maxRetryAttempts;
    private final PaginationOptions paginationOptions;
    // request with retry timeout
    private final Integer requestTimeoutSecs;
    private final Map<String, String> additonalHeaders;
    // client session
    private final AtomicReference<DatabendSession> databendSession;
    private final AtomicReference<QueryResults> currentResults = new AtomicReference<>(null);
    private static final Logger logger = Logger.getLogger(DatabendClientV1.class.getPackage().getName());

    private Consumer<DatabendSession> on_session_state_update;

    public DatabendClientV1(OkHttpClient httpClient, String sql, ClientSettings settings, Consumer<DatabendSession> on_session_state_update) {
        requireNonNull(httpClient, "httpClient is null");
        requireNonNull(sql, "sql is null");
        requireNonNull(settings, "settings is null");
        requireNonNull(settings.getHost(), "settings.host is null");
        this.httpClient = httpClient;
        this.query = sql;
        this.on_session_state_update = on_session_state_update;
        this.host = settings.getHost();
        this.paginationOptions = settings.getPaginationOptions();
        this.requestTimeoutSecs = settings.getQueryTimeoutSecs();
        this.additonalHeaders = settings.getAdditionalHeaders();
        this.maxRetryAttempts = settings.getRetryAttempts();
        // need atomic reference since it may get updated when query returned.
        this.databendSession = new AtomicReference<>(settings.getSession());
        Request request = buildQueryRequest(query, settings);
        boolean completed = this.execute(request);
        if (!completed) {
            throw new RuntimeException("Query failed to complete");
        }
    }

    public static List<DiscoveryNode> dicoverNodes(OkHttpClient httpClient, ClientSettings settings) {
        requireNonNull(httpClient, "httpClient is null");
        requireNonNull(settings, "settings is null");
        requireNonNull(settings.getHost(), "settings.host is null");
        Request request = buildDiscoveryRequest(settings);
        DiscoveryResponseCodec.DiscoveryResponse response = getDiscoveryResponse(httpClient, request, OptionalLong.empty(), settings.getQueryTimeoutSecs());
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
        HttpUrl url = HttpUrl.get(settings.getHost());
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
        Request.Builder builder = prepareRequest(url, this.additonalHeaders);
        return builder.post(okhttp3.RequestBody.create(MEDIA_TYPE_JSON, reqString)).build();
    }

    private static Request buildDiscoveryRequest(ClientSettings settings) {
        HttpUrl url = HttpUrl.get(settings.getHost());
        if (url == null) {
            // TODO(zhihanz) use custom exception
            throw new IllegalArgumentException("Invalid host: " + settings.getHost());
        }
        url = url.newBuilder().encodedPath(DISCOVERY_PATH).build();
        Request.Builder builder = prepareRequest(url, settings.getAdditionalHeaders());
        return builder.get().build();
    }

    @Override
    public String getQuery() {
        return query;
    }

    private static DiscoveryResponseCodec.DiscoveryResponse getDiscoveryResponse(OkHttpClient httpClient, Request request, OptionalLong materializedJsonSizeLimit, int requestTimeoutSecs) {
        requireNonNull(request, "request is null");

        long start = System.nanoTime();
        int attempts = 0;
        Exception lastException = null;

        while (true) {
            if (attempts > 0) {
                Duration sinceStart = Duration.ofNanos(System.nanoTime() - start);
                if (sinceStart.compareTo(Duration.ofSeconds(requestTimeoutSecs)) > 0) {
                    throw new RuntimeException(format("Error fetching discovery nodes (attempts: %s, duration: %s)", attempts, sinceStart.getSeconds()), lastException);
                }

                try {
                    MILLISECONDS.sleep(attempts * 100); // Exponential backoff
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted while fetching discovery nodes", e);
                }
            }
            attempts++;

            JsonResponse<DiscoveryResponseCodec.DiscoveryResponse> response;
            try {
                response = JsonResponse.execute(
                        DISCOVERY_RESULT_CODEC,
                        httpClient,
                        request,
                        materializedJsonSizeLimit);
            } catch (RuntimeException e) {
                lastException = e;
                if (e.getCause() instanceof ConnectException) {
                    // Retry on connection refused errors
                    continue;
                }
                throw new RuntimeException("Failed to fetch discovery nodes: " + e.getMessage(), e);
            }

            if (response.getStatusCode() == HTTP_OK && response.hasValue()) {
                DiscoveryResponseCodec.DiscoveryResponse discoveryResponse = response.getValue();
                if (discoveryResponse.getError() == null) {
                    return discoveryResponse; // Successful response
                }
                if (discoveryResponse.getError().notFound()) {
                    throw new UnsupportedOperationException("Discovery request feature not supported: " + discoveryResponse.getError());
                }
                throw new RuntimeException("Discovery request failed: " + discoveryResponse.getError());
            }

            // Handle other HTTP error codes and response body parsing for errors
            if (response.getResponseBody().isPresent()) {
                CloudErrors errors = CloudErrors.tryParse(response.getResponseBody().get());
                if (errors != null && errors.tryGetErrorKind().canRetry()) {
                    continue;
                }
            }

            if (response.getStatusCode() != 520) {
                throw new RuntimeException("Discovery request failed with status code: " + response.getStatusCode());
            }
        }
    }

    private boolean executeInternal(Request request, OptionalLong materializedJsonSizeLimit) {
        requireNonNull(request, "request is null");
        long start = System.nanoTime();
        long attempts = 0;
        Exception cause = null;
        while (true) {
            if (attempts > 0) {
                Duration sinceStart = Duration.ofNanos(System.nanoTime() - start);
                if (sinceStart.compareTo(Duration.ofSeconds(requestTimeoutSecs)) > 0) {
                    throw new RuntimeException(format("Error fetching next (attempts: %s, duration: %s)", attempts, sinceStart.getSeconds()), cause);
                }

                try {
                    MILLISECONDS.sleep(attempts * 100);
                } catch (InterruptedException e) {
                    try {
                        close();
                    } finally {
                        Thread.currentThread().interrupt();
                    }
                    throw new RuntimeException("StatementClient thread was interrupted");
                }
            }
            attempts++;
            JsonResponse<QueryResults> response;
            try {
                response = JsonResponse.execute(QUERY_RESULTS_CODEC, httpClient, request, materializedJsonSizeLimit);
            } catch (RuntimeException e) {
                if (e.getCause() instanceof ConnectException) {
                    // specially handle connection refused exception for retry purpose.
                    throw e;
                }
                throw new RuntimeException("Query failed: " + e.getMessage(), e);
            }

            if ((response.getStatusCode() == HTTP_OK) && response.hasValue() && (response.getValue().getError() == null)) {
                // q
                processResponse(response.getHeaders(), response.getValue());
                return true;
            }

            if (response.getResponseBody().isPresent()) {
                // try parse responseBody into ClientErrors
                CloudErrors errors = CloudErrors.tryParse(response.getResponseBody().get());
                if (errors != null) {
                    if (errors.tryGetErrorKind().canRetry()) {
                        continue;
                    } else {
                        throw new RuntimeException(String.valueOf(response.getValue().getError()));
                    }
                }
            }

            if (response.getStatusCode() != 520) {
                throw new RuntimeException("Query failed: " + response.getValue().getError());
            }
            return false;
        }
    }

    private String requestBodyToString(Request request) {
        try {
            final Request copy = request.newBuilder().build();
            final Buffer buffer = new Buffer();
            if (copy.body() != null) {
                copy.body().writeTo(buffer);
            }
            return buffer.readUtf8();
        } catch (final IOException e) {
            return "did not work";
        }
    }

    @Override
    public boolean execute(Request request) {
        return executeInternal(request, OptionalLong.empty());
    }

    private void processResponse(Headers headers, QueryResults results) {
        DatabendSession session = results.getSession();
        if (session != null) {
            databendSession.set(session);
            if (this.on_session_state_update != null) {
                this.on_session_state_update.accept(session);
            }
        }
        if (results.getQueryId() != null && this.additonalHeaders.get(ClientSettings.X_Databend_Query_ID) == null) {
            this.additonalHeaders.put(ClientSettings.X_Databend_Query_ID, results.getQueryId());
        }
        if (headers != null && headers.get(ClientSettings.X_DATABEND_ROUTE_HINT) != null) {
            this.additonalHeaders.put(ClientSettings.X_DATABEND_ROUTE_HINT, headers.get(ClientSettings.X_DATABEND_ROUTE_HINT));
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
        Request.Builder builder = prepareRequest(url, this.additonalHeaders);
        Request request = builder.get().build();
        return executeInternal(request, OptionalLong.of(MAX_MATERIALIZED_JSON_RESPONSE_SIZE));
    }

    @Override
    public boolean hasNext() {
        return !finished.get();
    }

    @Override
    public Map<String, String> getAdditionalHeaders() {
        return additonalHeaders;
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
    public String getHost() {
        return this.host;
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
        Request r = prepareRequest(url, this.additonalHeaders).get().build();
        try {
            httpClient.newCall(r).execute().close();
        } catch (IOException ignored) {
        }
    }
}
