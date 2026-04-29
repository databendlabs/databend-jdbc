package com.databend.client;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import okhttp3.Headers;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

@Test(timeOut = 10000)
public class TestDatabendClientV1 {
    @Test(groups = {"UNIT"})
    public void testPrepareRequestIncludesDefaultAndAdditionalHeaders() {
        Map<String, String> additionalHeaders = new HashMap<>();
        additionalHeaders.put("X-Test", "value");

        Request request = DatabendClientV1.prepareRequest(
                HttpUrl.get("http://127.0.0.1:8000/v1/query"),
                additionalHeaders)
                .get()
                .build();

        Headers headers = request.headers();
        Assert.assertEquals(headers.get("User-Agent"), DatabendClientV1.USER_AGENT_VALUE);
        Assert.assertEquals(headers.get("Accept"), "application/json");
        Assert.assertEquals(headers.get("Content-Type"), "application/json");
        Assert.assertEquals(headers.get("X-Test"), "value");
    }

    @Test(groups = {"UNIT"})
    public void testConstructorBackfillsQueryIdAndCloseIsIdempotent() throws Exception {
        AtomicInteger queryRequests = new AtomicInteger();
        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/v1/query", exchange -> {
            try {
                queryRequests.incrementAndGet();
                respondJson(exchange, queryResponse(
                        "server-qid",
                        "node-1",
                        "{\"database\":\"default\"}",
                        null,
                        null));
            }
            finally {
                exchange.close();
            }
        });
        server.start();

        try {
            Map<String, String> additionalHeaders = new HashMap<>();
            DatabendClientV1 client = new DatabendClientV1(
                    new OkHttpClient.Builder().build(),
                    "select 1",
                    new ClientSettings(serverBaseUrl(server), DatabendSession.createDefault(),
                            ClientSettings.DEFAULT_QUERY_TIMEOUT, ClientSettings.DEFAULT_CONNECTION_TIMEOUT,
                            ClientSettings.DEFAULT_SOCKET_TIMEOUT, PaginationOptions.defaultPaginationOptions(),
                            additionalHeaders, null, ClientSettings.DEFAULT_RETRY_ATTEMPTS),
                    null,
                    new AtomicReference<>());

            Assert.assertEquals(client.getAdditionalHeaders().get(ClientSettings.X_Databend_Query_ID), "server-qid");

            client.close();
            client.close();

            Assert.assertEquals(queryRequests.get(), 1);
        }
        finally {
            server.stop(0);
        }
    }

    @Test(groups = {"UNIT"})
    public void testAdvanceUsesStickyNodeAndPreservesExistingQueryId() throws Exception {
        AtomicReference<String> initialStickyHeader = new AtomicReference<>();
        AtomicReference<String> nextStickyHeader = new AtomicReference<>();
        AtomicReference<String> nextRouteHintHeader = new AtomicReference<>();
        AtomicReference<String> nextQueryIdHeader = new AtomicReference<>();

        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/v1/query", exchange -> {
            try {
                initialStickyHeader.set(exchange.getRequestHeaders().getFirst(ClientSettings.X_DATABEND_STICKY_NODE));
                exchange.getResponseHeaders().add(ClientSettings.X_DATABEND_ROUTE_HINT, "route-1");
                respondJson(exchange, queryResponse(
                        "server-qid",
                        "node-1",
                        "{\"database\":\"default\",\"need_sticky\":true}",
                        "/v1/query/next",
                        "/v1/query/final"));
            }
            finally {
                exchange.close();
            }
        });
        server.createContext("/v1/query/next", exchange -> {
            try {
                nextStickyHeader.set(exchange.getRequestHeaders().getFirst(ClientSettings.X_DATABEND_STICKY_NODE));
                nextRouteHintHeader.set(exchange.getRequestHeaders().getFirst(ClientSettings.X_DATABEND_ROUTE_HINT));
                nextQueryIdHeader.set(exchange.getRequestHeaders().getFirst(ClientSettings.X_Databend_Query_ID));
                respondJson(exchange, queryResponse(
                        "server-qid",
                        "node-1",
                        "{\"database\":\"default\",\"need_sticky\":true}",
                        "/v1/query/final",
                        "/v1/query/final"));
            }
            finally {
                exchange.close();
            }
        });
        server.start();

        try {
            Map<String, String> additionalHeaders = new HashMap<>();
            additionalHeaders.put(ClientSettings.X_Databend_Query_ID, "client-qid");
            AtomicReference<String> lastNodeId = new AtomicReference<>("node-prev");

            DatabendClientV1 client = new DatabendClientV1(
                    new OkHttpClient.Builder().build(),
                    "select 1",
                    new ClientSettings(serverBaseUrl(server),
                            new DatabendSession("default", null, null, true, false),
                            ClientSettings.DEFAULT_QUERY_TIMEOUT, ClientSettings.DEFAULT_CONNECTION_TIMEOUT,
                            ClientSettings.DEFAULT_SOCKET_TIMEOUT, PaginationOptions.defaultPaginationOptions(),
                            additionalHeaders, null, ClientSettings.DEFAULT_RETRY_ATTEMPTS),
                    null,
                    lastNodeId);

            Assert.assertEquals(initialStickyHeader.get(), "node-prev");
            Assert.assertEquals(client.getAdditionalHeaders().get(ClientSettings.X_Databend_Query_ID), "client-qid");
            Assert.assertEquals(client.getAdditionalHeaders().get(ClientSettings.X_DATABEND_ROUTE_HINT), "route-1");
            Assert.assertEquals(lastNodeId.get(), "node-1");

            Assert.assertTrue(client.advance());
            Assert.assertEquals(nextStickyHeader.get(), "node-1");
            Assert.assertEquals(nextRouteHintHeader.get(), "route-1");
            Assert.assertEquals(nextQueryIdHeader.get(), "client-qid");
        }
        finally {
            server.stop(0);
        }
    }

    private static String serverBaseUrl(HttpServer server) {
        return "http://127.0.0.1:" + server.getAddress().getPort();
    }

    private static String queryResponse(String queryId, String nodeId, String sessionJson, String nextUri, String finalUri) {
        return "{"
                + "\"id\":\"" + queryId + "\","
                + "\"node_id\":\"" + nodeId + "\","
                + "\"session\":" + sessionJson + ","
                + "\"schema\":[],"
                + "\"data\":[],"
                + "\"state\":\"Succeeded\","
                + "\"error\":null,"
                + "\"stats\":null,"
                + "\"affect\":null,"
                + "\"result_timeout_secs\":30,"
                + "\"stats_uri\":null,"
                + "\"final_uri\":" + quoteOrNull(finalUri) + ","
                + "\"next_uri\":" + quoteOrNull(nextUri) + ","
                + "\"kill_uri\":null"
                + "}";
    }

    private static String quoteOrNull(String value) {
        return value == null ? "null" : "\"" + value + "\"";
    }

    private static void respondJson(HttpExchange exchange, String json) throws IOException {
        byte[] body = json.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        exchange.getResponseHeaders().add("Content-Type", "application/json");
        exchange.sendResponseHeaders(200, body.length);
        exchange.getResponseBody().write(body);
    }
}
