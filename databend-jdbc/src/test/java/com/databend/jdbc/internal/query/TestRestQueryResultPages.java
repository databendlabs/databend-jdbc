package com.databend.jdbc.internal.query;

import com.databend.jdbc.internal.QueryResultFormat;
import com.databend.jdbc.internal.exception.DatabendQueryException;
import com.databend.jdbc.internal.session.PaginationOptions;
import com.databend.jdbc.internal.session.QueryRequestConfig;
import com.databend.jdbc.internal.session.SessionState;
import com.sun.net.httpserver.HttpServer;
import okhttp3.Interceptor;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Protocol;
import okhttp3.Response;
import okhttp3.ResponseBody;
import okio.Buffer;
import okio.BufferedSource;
import okio.Okio;
import okio.Source;
import okio.Timeout;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
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
    public void testMalformedArrowResponseRaisesDatabendQueryException() {
        AtomicInteger attempts = new AtomicInteger();
        MediaType arrowMediaType = MediaType.parse("application/vnd.apache.arrow.stream");
        OkHttpClient client = new OkHttpClient.Builder()
                .addInterceptor((Interceptor) chain -> {
                    attempts.incrementAndGet();
                    return arrowResponse(chain, ResponseBody.create(arrowMediaType, malformedArrowResponse()));
                })
                .build();

        DatabendQueryException exception = Assert.expectThrows(DatabendQueryException.class, () ->
                new RestQueryResultPages(
                        client,
                        "select 1",
                        requestConfig("http://127.0.0.1", QueryResultFormat.ARROW),
                        null,
                        new AtomicReference<>()));

        Assert.assertTrue(exception.getMessage().contains("Failed to execute query request"), exception.getMessage());
        Assert.assertNotNull(exception.getCause());
        Assert.assertTrue(exception.getCause().getMessage().contains("Failed to decode Arrow response"),
                exception.getCause().getMessage());
        Assert.assertEquals(attempts.get(), 1);
    }

    @Test(groups = {"UNIT_ARROW"})
    public void testTruncatedArrowBodyIsRetried() throws Exception {
        byte[] payload = arrowResponse();
        int thirdBatchCutoff = 864;
        AtomicInteger attempts = new AtomicInteger();
        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/v1/query", exchange -> {
            try {
                int attempt = attempts.incrementAndGet();
                exchange.getResponseHeaders().add("Content-Type", "application/vnd.apache.arrow.stream");
                exchange.sendResponseHeaders(200, 0);
                int bytesToWrite = attempt == 1 ? thirdBatchCutoff : payload.length;
                exchange.getResponseBody().write(payload, 0, bytesToWrite);
            }
            finally {
                exchange.close();
            }
        });
        server.start();

        try {
            RestQueryResultPages pages = new RestQueryResultPages(
                    new OkHttpClient(),
                    "select 42",
                    requestConfig(serverBaseUrl(server), QueryResultFormat.ARROW),
                    null,
                    new AtomicReference<>());

            Assert.assertEquals(attempts.get(), 2);
            Assert.assertEquals(pages.getResults().getQueryId(), "qid-arrow-retry");
            Assert.assertEquals(pages.getPage().getRowCount(), 3);
            Assert.assertEquals(pages.getPage().getValue(0, 0), 40);
            Assert.assertEquals(pages.getPage().getValue(1, 0), 41);
            Assert.assertEquals(pages.getPage().getValue(2, 0), 42);
            pages.close();
        }
        finally {
            server.stop(0);
        }
    }

    @Test(groups = {"UNIT_ARROW"})
    public void testArrowTransportFailureBeforeHeaderIsRetried() throws Exception {
        byte[] payload = arrowResponse();
        AtomicInteger attempts = new AtomicInteger();
        MediaType arrowMediaType = MediaType.parse("application/vnd.apache.arrow.stream");
        OkHttpClient client = new OkHttpClient.Builder()
                .addInterceptor((Interceptor) chain -> {
                    ResponseBody body = attempts.incrementAndGet() == 1
                            ? timeoutResponseBody(arrowMediaType)
                            : ResponseBody.create(arrowMediaType, payload);
                    return new Response.Builder()
                            .request(chain.request())
                            .protocol(Protocol.HTTP_1_1)
                            .code(200)
                            .message("OK")
                            .body(body)
                            .build();
                })
                .build();

        RestQueryResultPages pages = new RestQueryResultPages(
                client,
                "select 42",
                requestConfig("http://127.0.0.1", QueryResultFormat.ARROW),
                null,
                new AtomicReference<>());

        Assert.assertEquals(attempts.get(), 2);
        Assert.assertEquals(pages.getPage().getRowCount(), 3);
        Assert.assertEquals(pages.getPage().getValue(0, 0), 40);
        Assert.assertEquals(pages.getPage().getValue(1, 0), 41);
        Assert.assertEquals(pages.getPage().getValue(2, 0), 42);
        pages.close();
    }

    @Test(groups = {"UNIT_ARROW"})
    public void testTruncatedArrowHeaderIsNotRetried() {
        byte[] payload = arrowResponse();
        AtomicInteger attempts = new AtomicInteger();
        MediaType arrowMediaType = MediaType.parse("application/vnd.apache.arrow.stream");
        OkHttpClient client = new OkHttpClient.Builder()
                .addInterceptor((Interceptor) chain -> {
                    attempts.incrementAndGet();
                    byte[] responsePayload = Arrays.copyOf(payload, 64);
                    return arrowResponse(chain, ResponseBody.create(arrowMediaType, responsePayload));
                })
                .build();

        DatabendQueryException exception = Assert.expectThrows(DatabendQueryException.class, () ->
                new RestQueryResultPages(
                        client,
                        "select 42",
                        requestConfig("http://127.0.0.1", QueryResultFormat.ARROW),
                        null,
                        new AtomicReference<>()));

        Assert.assertEquals(attempts.get(), 1);
        Assert.assertTrue(exception.getCause() instanceof java.sql.SQLException,
                exception.getCause().toString());
        Assert.assertTrue(exception.getCause().getMessage().contains("Failed to decode Arrow response"),
                exception.getCause().getMessage());
    }

    @Test(groups = {"UNIT_ARROW"})
    public void testArrowReaderCloseFailureIsRetried() throws Exception {
        byte[] payload = arrowResponse();
        AtomicInteger attempts = new AtomicInteger();
        MediaType arrowMediaType = MediaType.parse("application/vnd.apache.arrow.stream");
        OkHttpClient client = new OkHttpClient.Builder()
                .addInterceptor((Interceptor) chain -> {
                    ResponseBody body = attempts.incrementAndGet() == 1
                            ? closeFailureResponseBody(arrowMediaType, payload)
                            : ResponseBody.create(arrowMediaType, payload);
                    return arrowResponse(chain, body);
                })
                .build();

        RestQueryResultPages pages = new RestQueryResultPages(
                client,
                "select 42",
                requestConfig("http://127.0.0.1", QueryResultFormat.ARROW),
                null,
                new AtomicReference<>());

        Assert.assertEquals(attempts.get(), 2);
        Assert.assertEquals(pages.getPage().getRowCount(), 3);
        Assert.assertEquals(pages.getPage().getValue(0, 0), 40);
        Assert.assertEquals(pages.getPage().getValue(1, 0), 41);
        Assert.assertEquals(pages.getPage().getValue(2, 0), 42);
        pages.close();
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
        return requestConfig(host, QueryResultFormat.JSON);
    }

    private static QueryRequestConfig requestConfig(String host, QueryResultFormat resultFormat) {
        return new QueryRequestConfig(
                host,
                SessionState.createDefault(),
                QueryRequestConfig.DEFAULT_QUERY_TIMEOUT,
                QueryRequestConfig.DEFAULT_CONNECTION_TIMEOUT,
                QueryRequestConfig.DEFAULT_SOCKET_TIMEOUT,
                resultFormat,
                PaginationOptions.defaultPaginationOptions(),
                new HashMap<String, String>(),
                null,
                QueryRequestConfig.DEFAULT_RETRY_ATTEMPTS);
    }

    private static String serverBaseUrl(HttpServer server) {
        return "http://127.0.0.1:" + server.getAddress().getPort();
    }

    private static ResponseBody timeoutResponseBody(MediaType contentType) {
        BufferedSource source = Okio.buffer(new Source() {
            @Override
            public long read(Buffer sink, long byteCount) throws IOException {
                throw new SocketTimeoutException("timed out while reading Arrow schema");
            }

            @Override
            public Timeout timeout() {
                return Timeout.NONE;
            }

            @Override
            public void close() {
            }
        });
        return ResponseBody.create(contentType, -1, source);
    }

    private static ResponseBody closeFailureResponseBody(MediaType contentType, byte[] payload) {
        Buffer buffer = new Buffer().write(payload);
        BufferedSource source = Okio.buffer(new Source() {
            @Override
            public long read(Buffer sink, long byteCount) {
                return buffer.read(sink, byteCount);
            }

            @Override
            public Timeout timeout() {
                return Timeout.NONE;
            }

            @Override
            public void close() throws IOException {
                throw new IOException("unexpected end of stream while closing Arrow body");
            }
        });
        return ResponseBody.create(contentType, payload.length, source);
    }

    private static Response arrowResponse(Interceptor.Chain chain, ResponseBody body) {
        return new Response.Builder()
                .request(chain.request())
                .protocol(Protocol.HTTP_1_1)
                .code(200)
                .message("OK")
                .body(body)
                .build();
    }

    private static byte[] malformedArrowResponse() {
        // Complete IPC framing: continuation marker, metadata length 8, then eight bytes
        // of invalid FlatBuffer metadata. This is malformed, not a truncated stream.
        return new byte[] {-1, -1, -1, -1, 8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
    }

    private static byte[] arrowResponse() {
        // Generated with ArrowStreamWriter using an Int32 field named "n": call
        // writer.start(), then for values 40, 41, and 42 set one row and call
        // writer.writeBatch(), followed by writer.end(). Record output.size() after start,
        // each batch, and end: schema=464, batches=624/784/944, stream=952. The retry test
        // cuts at byte 864 so two complete ArrowRecordBatches must be released before retry.
        // Keep generation out of this test because ArrowStreamWriter is removed from the
        // minimized release driver jar.
        return Base64.getDecoder().decode(
                "/////8gBAAAQAAAAAAAKAA4ABgANAAgACgAAAAAABAAQAAAAAAEKAAwAAAAIAAQACgAAAAgAAAA8AQAAAQAAAAwAAAAIAAwA"
                        + "CAAEAAgAAAAIAAAADAEAAAIBAAB7ImlkIjoicWlkLWFycm93LXJldHJ5Iiwibm9kZV9pZCI6Im5vZGUiLCJzZXNzaW9uIjp7"
                        + "ImRhdGFiYXNlIjoiZGVmYXVsdCJ9LCJzY2hlbWEiOltdLCJkYXRhIjpbXSwic3RhdGUiOiJSdW5uaW5nIiwiZXJyb3IiOm51"
                        + "bGwsInN0YXRzIjpudWxsLCJhZmZlY3QiOm51bGwsInJlc3VsdF90aW1lb3V0X3NlY3MiOjMwLCJzdGF0c191cmkiOm51bGws"
                        + "ImZpbmFsX3VyaSI6Ii92MS9xdWVyeS9maW5hbCIsIm5leHRfdXJpIjpudWxsLCJraWxsX3VyaSI6bnVsbH0AAA8AAAByZXNw"
                        + "b25zZV9oZWFkZXIAAQAAABgAAAAAABIAGAAUAAAAEwAMAAAACAAEABIAAAAUAAAAFAAAABwAAAAAAAACIAAAAAAAAAAAAAAA"
                        + "CAAMAAgABwAIAAAAAAAAASAAAAABAAAAbgAAAAAAAAD/////iAAAABQAAAAAAAAADAAWAA4AFQAQAAQADAAAABAAAAAAAAAA"
                        + "AAAEABAAAAAAAwoAGAAMAAgABAAKAAAAFAAAADgAAAABAAAAAAAAAAAAAAACAAAAAAAAAAAAAAABAAAAAAAAAAgAAAAAAAAA"
                        + "BAAAAAAAAAAAAAAAAQAAAAEAAAAAAAAAAAAAAAAAAAABAAAAAAAAACgAAAAAAAAA/////4gAAAAUAAAAAAAAAAwAFgAOABUA"
                        + "EAAEAAwAAAAQAAAAAAAAAAAABAAQAAAAAAMKABgADAAIAAQACgAAABQAAAA4AAAAAQAAAAAAAAAAAAAAAgAAAAAAAAAAAAAA"
                        + "AQAAAAAAAAAIAAAAAAAAAAQAAAAAAAAAAAAAAAEAAAABAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAApAAAAAAAAAP////+IAAAA"
                        + "FAAAAAAAAAAMABYADgAVABAABAAMAAAAEAAAAAAAAAAAAAQAEAAAAAADCgAYAAwACAAEAAoAAAAUAAAAOAAAAAEAAAAAAAAA"
                        + "AAAAAAIAAAAAAAAAAAAAAAEAAAAAAAAACAAAAAAAAAAEAAAAAAAAAAAAAAABAAAAAQAAAAAAAAAAAAAAAAAAAAEAAAAAAAAA"
                        + "KgAAAAAAAAD/////AAAAAA==");
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
