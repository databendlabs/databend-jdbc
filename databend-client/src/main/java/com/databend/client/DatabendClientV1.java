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
import okhttp3.Headers;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;

import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.time.Duration;
import java.util.Locale;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicReference;
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
    public static final String USER_AGENT_VALUE = DatabendClientV1.class.getSimpleName() +
            "/" +
            firstNonNull(DatabendClientV1.class.getPackage().getImplementationVersion(), "jvm-unknown");
    public static final MediaType MEDIA_TYPE_JSON = MediaType.parse("application/json; charset=utf-8");
    public static final JsonCodec<QueryResults> QUERY_RESULTS_CODEC = jsonCodec(QueryResults.class);

    public static final String QUERY_PATH = "/v1/query";
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
    private final AtomicReference<QueryResults> currentResults = new AtomicReference<>();
    private static final Logger logger = Logger.getLogger(DatabendClientV1.class.getPackage().getName());

    public DatabendClientV1(OkHttpClient httpClient, String sql, ClientSettings settings) {
        requireNonNull(httpClient, "httpClient is null");
        requireNonNull(sql, "sql is null");
        requireNonNull(settings, "settings is null");
        requireNonNull(settings.getHost(), "settings.host is null");
        this.httpClient = httpClient;
        this.query = sql;
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

    public Request.Builder prepareRequst(HttpUrl url) {
        Request.Builder builder = new Request.Builder()
                .url(url)
                .header("User-Agent", USER_AGENT_VALUE)
                .header("Accept", "application/json")
                .header("Content-Type", "application/json");
        if (this.getAdditionalHeaders() != null) {
            this.getAdditionalHeaders().forEach(builder::addHeader);
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
        Request.Builder builder = prepareRequst(url);
        return builder.post(okhttp3.RequestBody.create(MEDIA_TYPE_JSON, reqString)).build();
    }

    @Override
    public String getQuery() {
        return query;
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
                cause = e;
                continue;
            }

            if ((response.getStatusCode() == HTTP_OK) && response.hasValue()) {
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
                        throw new RuntimeException(errors.toString());
                    }
                }
            }

            if (response.getStatusCode() != 520) {
                throw new RuntimeException("Query failed: " + response.getResponseBody());
            }
            return false;
        }
    }

    @Override
    public boolean execute(Request request) {
        return executeInternal(request, OptionalLong.empty());
    }

    private void processResponse(Headers headers, QueryResults results) {
        if (results.getSession() != null) {
            databendSession.set(results.getSession());
        }
        if (results.getQueryId() != null && this.additonalHeaders.get(ClientSettings.X_Databend_Query_ID) == null) {
            this.additonalHeaders.put(ClientSettings.X_Databend_Query_ID, results.getQueryId());
        }
        currentResults.set(results);
    }

    @Override
    public boolean next() {
        requireNonNull(this.host, "host is null");
        requireNonNull(this.currentResults.get(), "currentResults is null");
        if (this.currentResults.get().getNextUri() == null) {
            // no need to fetch next page
            return false;
        }

        String nextUriPath = this.currentResults.get().getNextUri().toString();
        HttpUrl url = HttpUrl.get(this.host);
        url = url.newBuilder().encodedPath(nextUriPath).build();
        Request.Builder builder = prepareRequst(url);
        Request request = builder.get().build();
        return executeInternal(request, OptionalLong.of(MAX_MATERIALIZED_JSON_RESPONSE_SIZE));
    }

    @Override
    public boolean isRunning() {
        QueryResults results = this.currentResults.get();
        if (results == null) {
            return false;
        }
        // if State is Failed or Finished, then it is not running
        if (results.getState().toLowerCase(Locale.US).equals("failed") || results.getState().toLowerCase(Locale.US).equals("finished")) {
            return false;
        }
        // is running if nextUri is not null
        return results.getNextUri() != null;
    }

    @Override
    public  Map<String, String> getAdditionalHeaders() {
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
    public void close() {
        killQuery();
    }

    private void killQuery() {
        QueryResults q = this.currentResults.get();
        if (q == null) {
            return;
        }
        if (q.getKillUri() == null) {
            return;
        }
        String killUriPath = q.getKillUri().toString();
        HttpUrl url = HttpUrl.get(this.host);
        url = url.newBuilder().encodedPath(killUriPath).build();
        Request r = prepareRequst(url).get().build();
        try {
            httpClient.newCall(r).execute().close();
        } catch (IOException ignored) {

        }
    }
}
