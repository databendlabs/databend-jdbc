package com.databend.jdbc.internal.query;

import com.databend.jdbc.internal.QueryResultFormat;
import com.databend.jdbc.internal.exception.DatabendQueryException;
import com.databend.jdbc.internal.session.PaginationOptions;
import com.databend.jdbc.internal.session.QueryRequestConfig;
import com.databend.jdbc.internal.session.SessionState;
import com.sun.net.httpserver.HttpServer;
import okhttp3.Interceptor;
import okhttp3.OkHttpClient;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

@Test(timeOut = 10000)
public class TestRestQueryResultPages {
    @Test(groups = {"UNIT"})
    public void testMalformedJsonResponseRaisesDatabendQueryException() throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/v1/query", exchange -> {
            try {
                byte[] payload = "{\"id\":".getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, payload.length);
                exchange.getResponseBody().write(payload);
            }
            finally {
                exchange.close();
            }
        });
        server.start();

        try {
            DatabendQueryException exception = Assert.expectThrows(DatabendQueryException.class, () ->
                    new RestQueryResultPages(
                            new OkHttpClient(),
                            "select 1",
                            requestConfig(serverBaseUrl(server)),
                            null,
                            new AtomicReference<>()));

            Assert.assertTrue(exception.getMessage().contains("Failed to decode query response"), exception.getMessage());
            Assert.assertNotNull(exception.getCause(), exception.getMessage());
            Assert.assertTrue(exception.getCause() instanceof IllegalArgumentException, exception.getCause().toString());
            Assert.assertTrue(exception.getCause().getMessage().contains("Unable to create class com.databend.jdbc.internal.query.QueryResults"),
                    exception.getCause().getMessage());
        }
        finally {
            server.stop(0);
        }
    }

    @Test(groups = {"UNIT"})
    public void testServerQueryErrorResponseRaisesDatabendQueryException() throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/v1/query", exchange -> {
            try {
                byte[] payload = ("{"
                        + "\"id\":\"qid\","
                        + "\"node_id\":\"node\","
                        + "\"session\":{\"database\":\"default\"},"
                        + "\"schema\":[],"
                        + "\"data\":[],"
                        + "\"state\":\"Failed\","
                        + "\"error\":{\"code\":1065,\"message\":\"syntax error\"},"
                        + "\"stats\":null,"
                        + "\"affect\":null,"
                        + "\"result_timeout_secs\":30,"
                        + "\"stats_uri\":null,"
                        + "\"final_uri\":null,"
                        + "\"next_uri\":null,"
                        + "\"kill_uri\":null"
                        + "}").getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, payload.length);
                exchange.getResponseBody().write(payload);
            }
            finally {
                exchange.close();
            }
        });
        server.start();

        try {
            DatabendQueryException exception = Assert.expectThrows(DatabendQueryException.class, () ->
                    new RestQueryResultPages(
                            new OkHttpClient(),
                            "select broken",
                            requestConfig(serverBaseUrl(server)),
                            null,
                            new AtomicReference<>()));

            Assert.assertTrue(exception.getMessage().contains("Query Failed"), exception.getMessage());
            Assert.assertTrue(exception.getMessage().contains("syntax error"), exception.getMessage());
        }
        finally {
            server.stop(0);
        }
    }

    @Test(groups = {"UNIT_ARROW"})
    public void testMalformedArrowResponseRaisesDatabendQueryException() throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/v1/query", exchange -> {
            try {
                byte[] payload = "not-arrow".getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "application/vnd.apache.arrow.stream");
                exchange.sendResponseHeaders(200, payload.length);
                exchange.getResponseBody().write(payload);
            }
            finally {
                exchange.close();
            }
        });
        server.start();

        try {
            DatabendQueryException exception = Assert.expectThrows(DatabendQueryException.class, () ->
                    new RestQueryResultPages(
                            new OkHttpClient(),
                            "select 1",
                            requestConfig(serverBaseUrl(server)),
                            null,
                            new AtomicReference<>()));

            Assert.assertTrue(exception.getMessage().contains("Failed to execute query request"), exception.getMessage());
            Assert.assertNotNull(exception.getCause());
            Assert.assertTrue(exception.getCause().getMessage().contains("Failed to decode Arrow response"),
                    exception.getCause().getMessage());
        }
        finally {
            server.stop(0);
        }
    }

    @Test(groups = {"UNIT"})
    public void testInitialQueryConnectFailureThenRetrySucceeds() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        AtomicInteger serverAttempts = new AtomicInteger();
        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/v1/query", exchange -> {
            try {
                serverAttempts.incrementAndGet();
                byte[] payload = queryResponse("qid-retry", null, null).getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, payload.length);
                exchange.getResponseBody().write(payload);
            }
            finally {
                exchange.close();
            }
        });
        server.start();

        try {
            OkHttpClient client = new OkHttpClient.Builder()
                    .addInterceptor((Interceptor) chain -> {
                        if ("/v1/query".equals(chain.request().url().encodedPath()) && attempts.incrementAndGet() == 1) {
                            throw new IOException("connection refused", new ConnectException("Connection refused"));
                        }
                        return chain.proceed(chain.request());
                    })
                    .build();

            RestQueryResultPages pages = new RestQueryResultPages(
                    client,
                    "select 1",
                    requestConfig(serverBaseUrl(server)),
                    null,
                    new AtomicReference<>());

            Assert.assertEquals(attempts.get(), 2);
            Assert.assertEquals(serverAttempts.get(), 1);
            Assert.assertEquals(pages.getResults().getQueryId(), "qid-retry");
            Assert.assertNull(pages.getResults().getNextUri());
        }
        finally {
            server.stop(0);
        }
    }

    @Test(groups = {"UNIT"})
    public void testAdvanceMalformedNextPageRaisesDatabendQueryException() throws Exception {
        AtomicReference<String> initialQueryId = new AtomicReference<>();
        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/v1/query", exchange -> {
            try {
                if ("POST".equals(exchange.getRequestMethod())) {
                    byte[] payload = ("{"
                            + "\"id\":\"qid-1\","
                            + "\"node_id\":\"node\","
                            + "\"session\":{\"database\":\"default\"},"
                            + "\"schema\":[{\"name\":\"c1\",\"type\":\"String\"}],"
                            + "\"data\":[[\"v1\"]],"
                            + "\"state\":\"Running\","
                            + "\"error\":null,"
                            + "\"stats\":null,"
                            + "\"affect\":null,"
                            + "\"result_timeout_secs\":30,"
                            + "\"stats_uri\":null,"
                            + "\"final_uri\":\"/v1/query/final\","
                            + "\"next_uri\":\"/v1/query/next\","
                            + "\"kill_uri\":null"
                            + "}").getBytes(StandardCharsets.UTF_8);
                    exchange.getResponseHeaders().add("Content-Type", "application/json");
                    exchange.sendResponseHeaders(200, payload.length);
                    exchange.getResponseBody().write(payload);
                    return;
                }
                initialQueryId.set(exchange.getRequestHeaders().getFirst(QueryRequestConfig.X_DATABEND_QUERY_ID));
                byte[] payload = "{\"id\":".getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, payload.length);
                exchange.getResponseBody().write(payload);
            }
            finally {
                exchange.close();
            }
        });
        server.start();

        try {
            RestQueryResultPages pages = new RestQueryResultPages(
                    new OkHttpClient(),
                    "select 1",
                    requestConfig(serverBaseUrl(server)),
                    null,
                    new AtomicReference<>());

            DatabendQueryException exception = Assert.expectThrows(DatabendQueryException.class, pages::advance);

            Assert.assertEquals(initialQueryId.get(), "qid-1");
            Assert.assertTrue(exception.getMessage().contains("Failed to decode query response"), exception.getMessage());
        }
        finally {
            server.stop(0);
        }
    }

    @Test(groups = {"UNIT"})
    public void testAdvanceUnexpectedEndOfStreamThenRetrySucceeds() throws Exception {
        AtomicReference<String> initialQueryId = new AtomicReference<>();
        AtomicInteger nextPageAttempts = new AtomicInteger();
        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/v1/query", exchange -> {
            try {
                if ("POST".equals(exchange.getRequestMethod())) {
                    byte[] payload = queryResponse("qid-stream", "/v1/query/next-retry", "v1").getBytes(StandardCharsets.UTF_8);
                    exchange.getResponseHeaders().add("Content-Type", "application/json");
                    exchange.sendResponseHeaders(200, payload.length);
                    exchange.getResponseBody().write(payload);
                    return;
                }
                initialQueryId.set(exchange.getRequestHeaders().getFirst(QueryRequestConfig.X_DATABEND_QUERY_ID));
                byte[] payload = queryResponse("qid-stream", "/v1/query/final", "v2").getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, payload.length);
                exchange.getResponseBody().write(payload);
            }
            finally {
                exchange.close();
            }
        });
        server.start();

        try {
            OkHttpClient client = new OkHttpClient.Builder()
                    .addInterceptor((Interceptor) chain -> {
                        if ("/v1/query/next-retry".equals(chain.request().url().encodedPath())
                                && nextPageAttempts.incrementAndGet() == 1) {
                            throw new IOException("unexpected end of stream");
                        }
                        return chain.proceed(chain.request());
                    })
                    .build();

            RestQueryResultPages pages = new RestQueryResultPages(
                    client,
                    "select 1",
                    requestConfig(serverBaseUrl(server)),
                    null,
                    new AtomicReference<>());

            Assert.assertTrue(pages.advance());
            Assert.assertEquals(nextPageAttempts.get(), 2);
            Assert.assertEquals(initialQueryId.get(), "qid-stream");
            Assert.assertEquals(pages.getResults().getDataRaw().get(0).get(0), "v2");
        }
        finally {
            server.stop(0);
        }
    }

    @Test(groups = {"UNIT"})
    public void testAdvanceRetryable503ExhaustionRaisesDatabendQueryException() throws Exception {
        AtomicReference<String> initialQueryId = new AtomicReference<>();
        AtomicReference<Integer> nextPageAttempts = new AtomicReference<>(0);
        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/v1/query", exchange -> {
            try {
                if ("POST".equals(exchange.getRequestMethod())) {
                    byte[] payload = ("{"
                            + "\"id\":\"qid-503\","
                            + "\"node_id\":\"node\","
                            + "\"session\":{\"database\":\"default\"},"
                            + "\"schema\":[{\"name\":\"c1\",\"type\":\"String\"}],"
                            + "\"data\":[[\"v1\"]],"
                            + "\"state\":\"Running\","
                            + "\"error\":null,"
                            + "\"stats\":null,"
                            + "\"affect\":null,"
                            + "\"result_timeout_secs\":30,"
                            + "\"stats_uri\":null,"
                            + "\"final_uri\":\"/v1/query/final\","
                            + "\"next_uri\":\"/v1/query/next-503\","
                            + "\"kill_uri\":null"
                            + "}").getBytes(StandardCharsets.UTF_8);
                    exchange.getResponseHeaders().add("Content-Type", "application/json");
                    exchange.sendResponseHeaders(200, payload.length);
                    exchange.getResponseBody().write(payload);
                    return;
                }
                initialQueryId.set(exchange.getRequestHeaders().getFirst(QueryRequestConfig.X_DATABEND_QUERY_ID));
                nextPageAttempts.set(nextPageAttempts.get() + 1);
                byte[] payload = "{\"error\":\"temporary\"}".getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "application/json");
                exchange.sendResponseHeaders(503, payload.length);
                exchange.getResponseBody().write(payload);
            }
            finally {
                exchange.close();
            }
        });
        server.start();

        try {
            RestQueryResultPages pages = new RestQueryResultPages(
                    new OkHttpClient(),
                    "select 1",
                    requestConfig(serverBaseUrl(server)),
                    null,
                    new AtomicReference<>());

            DatabendQueryException exception = Assert.expectThrows(DatabendQueryException.class, pages::advance);

            Assert.assertEquals(initialQueryId.get(), "qid-503");
            Assert.assertEquals(nextPageAttempts.get().intValue(), 3);
            Assert.assertTrue(exception.getMessage().contains("Failed to execute query request"), exception.getMessage());
            Assert.assertNotNull(exception.getCause());
            Assert.assertTrue(exception.getCause().getMessage().contains("status_code = 503"),
                    exception.getCause().getMessage());
        }
        finally {
            server.stop(0);
        }
    }

    @Test(groups = {"UNIT"})
    public void testInitialQueryRetryExhaustionOnNetworkErrorRaisesDatabendQueryException() {
        AtomicInteger attempts = new AtomicInteger();
        OkHttpClient client = new OkHttpClient.Builder()
                .addInterceptor((Interceptor) chain -> {
                    if ("/v1/query".equals(chain.request().url().encodedPath())) {
                        attempts.incrementAndGet();
                        throw new IOException("connection refused", new ConnectException("Connection refused"));
                    }
                    return chain.proceed(chain.request());
                })
                .build();

        DatabendQueryException exception = Assert.expectThrows(DatabendQueryException.class, () ->
                new RestQueryResultPages(
                        client,
                        "select 1",
                        requestConfig("http://127.0.0.1:1"),
                        null,
                        new AtomicReference<>()));

        Assert.assertEquals(attempts.get(), 3);
        Assert.assertTrue(exception.getMessage().contains("Failed to execute query request"), exception.getMessage());
        Assert.assertNotNull(exception.getCause());
        Assert.assertTrue(exception.getCause().getMessage().contains("after 3 attempts"),
                exception.getCause().getMessage());
    }

    private static QueryRequestConfig requestConfig(String host) {
        return new QueryRequestConfig(
                host,
                SessionState.createDefault(),
                QueryRequestConfig.DEFAULT_QUERY_TIMEOUT,
                QueryRequestConfig.DEFAULT_CONNECTION_TIMEOUT,
                QueryRequestConfig.DEFAULT_SOCKET_TIMEOUT,
                QueryResultFormat.JSON,
                PaginationOptions.defaultPaginationOptions(),
                new HashMap<String, String>(),
                null,
                QueryRequestConfig.DEFAULT_RETRY_ATTEMPTS);
    }

    private static String serverBaseUrl(HttpServer server) {
        return "http://127.0.0.1:" + server.getAddress().getPort();
    }

    private static String queryResponse(String queryId, String nextUri, String value) {
        String schema = value == null ? "[]" : "[{\"name\":\"c1\",\"type\":\"String\"}]";
        String data = value == null ? "[]" : "[[\"" + value + "\"]]";
        String nextUriJson = nextUri == null ? "null" : "\"" + nextUri + "\"";
        return "{"
                + "\"id\":\"" + queryId + "\","
                + "\"node_id\":\"node\","
                + "\"session\":{\"database\":\"default\"},"
                + "\"schema\":" + schema + ","
                + "\"data\":" + data + ","
                + "\"state\":\"Running\","
                + "\"error\":null,"
                + "\"stats\":null,"
                + "\"affect\":null,"
                + "\"result_timeout_secs\":30,"
                + "\"stats_uri\":null,"
                + "\"final_uri\":\"/v1/query/final\","
                + "\"next_uri\":" + nextUriJson + ","
                + "\"kill_uri\":null"
                + "}";
    }
}
