package com.databend.jdbc.internal.session;

import com.databend.jdbc.DriverInfo;
import com.databend.jdbc.internal.QueryResultFormat;
import com.databend.jdbc.internal.query.QueryResultPages;
import okhttp3.OkHttpClient;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicReference;

import static com.databend.jdbc.internal.http.OkHttpUtils.userAgentInterceptor;

@Test(timeOut = 10000)
public class TestDatabendSessionHandle {
    @Test(groups = {"UNIT"})
    public void testUploadStreamUsesStageUploadEndpointWhenPresignDisabled() throws Exception {
        AtomicReference<String> method = new AtomicReference<>();
        AtomicReference<String> path = new AtomicReference<>();
        AtomicReference<String> stageName = new AtomicReference<>();
        AtomicReference<String> relativePath = new AtomicReference<>();
        AtomicReference<String> userAgent = new AtomicReference<>();
        AtomicReference<String> warehouse = new AtomicReference<>();
        AtomicReference<String> body = new AtomicReference<>();

        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/v1/upload_to_stage", exchange -> {
            try {
                captureRequest(exchange, method, path, stageName, relativePath, userAgent, warehouse, body);
            }
            finally {
                exchange.close();
            }
        });
        server.start();

        try {
            DatabendSessionHandle handle = new DatabendSessionHandle(
                    new OkHttpClient.Builder()
                            .addInterceptor(userAgentInterceptor(DriverInfo.USER_AGENT_VALUE))
                            .build(),
                    SessionHandleConfig.builder()
                            .setBaseUri(URI.create("http://127.0.0.1:" + server.getAddress().getPort()))
                            .setWarehouse("test_wh")
                            .setInitialSession(SessionState.createDefault())
                            .build(),
                    null);
            handle.initializePresign("off", false);

            byte[] payload = "1234".getBytes(StandardCharsets.UTF_8);
            handle.uploadStream(
                    "test_stage",
                    "dir1",
                    new ByteArrayInputStream(payload),
                    "f1.txt",
                    payload.length,
                    false);

            Assert.assertEquals(method.get(), "PUT");
            Assert.assertEquals(path.get(), "/v1/upload_to_stage");
            Assert.assertEquals(stageName.get(), "test_stage");
            Assert.assertEquals(relativePath.get(), "dir1/");
            Assert.assertEquals(userAgent.get(), DriverInfo.USER_AGENT_VALUE);
            Assert.assertEquals(warehouse.get(), "test_wh");
            Assert.assertTrue(body.get().contains("name=\"upload\""));
            Assert.assertTrue(body.get().contains("filename=\"f1.txt\""));
            Assert.assertTrue(body.get().contains("1234"));
        }
        finally {
            server.stop(0);
        }
    }

    @Test(groups = {"UNIT"})
    public void testLoginEnablesArrowTransportWhenServerAdvertisesArrowResultVersion() throws Exception {
        AtomicReference<String> accept = new AtomicReference<>();
        AtomicReference<String> requestBody = new AtomicReference<>();

        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/v1/session/login", exchange -> {
            try {
                byte[] response = "{\"version\":\"1.2.100\",\"server_max_arrow_result_version\":2}".getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, response.length);
                exchange.getResponseBody().write(response);
            }
            finally {
                exchange.close();
            }
        });
        server.createContext("/v1/query", exchange -> {
            try {
                accept.set(exchange.getRequestHeaders().getFirst("Accept"));
                requestBody.set(new String(readAllBytes(exchange), StandardCharsets.UTF_8));
                byte[] response = "{\"id\":\"qid\",\"node_id\":\"node\",\"session\":{\"database\":\"default\"},\"schema\":[],\"data\":[]}".getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, response.length);
                exchange.getResponseBody().write(response);
            }
            finally {
                exchange.close();
            }
        });
        server.start();

        try {
            DatabendSessionHandle handle = new DatabendSessionHandle(
                    new OkHttpClient.Builder()
                            .addInterceptor(userAgentInterceptor(DriverInfo.USER_AGENT_VALUE))
                            .build(),
                    SessionHandleConfig.builder()
                            .setBaseUri(URI.create("http://127.0.0.1:" + server.getAddress().getPort()))
                            .setInitialSession(SessionState.createDefault())
                            .setQueryResultFormat(QueryResultFormat.ARROW)
                            .setQueryTimeoutSecs(30)
                            .setConnectionTimeoutSecs(30)
                            .setSocketTimeoutSecs(60)
                            .setWaitTimeSecs(10)
                            .setMaxRowsInBuffer(1000)
                            .setMaxRowsPerPage(1000)
                            .build(),
                    null);
            handle.login();
            QueryResultPages pages = handle.startQuery("qid", "select 1", null, null);
            pages.close();

            Assert.assertEquals(accept.get(), "application/vnd.apache.arrow.stream");
            Assert.assertTrue(requestBody.get().contains("\"arrow_result_version_max\":2"));
        }
        finally {
            server.stop(0);
        }
    }

    private static void captureRequest(
            HttpExchange exchange,
            AtomicReference<String> method,
            AtomicReference<String> path,
            AtomicReference<String> stageName,
            AtomicReference<String> relativePath,
            AtomicReference<String> userAgent,
            AtomicReference<String> warehouse,
            AtomicReference<String> body) throws IOException {
        method.set(exchange.getRequestMethod());
        path.set(exchange.getRequestURI().getPath());
        stageName.set(exchange.getRequestHeaders().getFirst(QueryRequestConfig.X_DATABEND_STAGE_NAME));
        relativePath.set(exchange.getRequestHeaders().getFirst(QueryRequestConfig.X_DATABEND_RELATIVE_PATH));
        userAgent.set(exchange.getRequestHeaders().getFirst("User-Agent"));
        warehouse.set(exchange.getRequestHeaders().getFirst(QueryRequestConfig.DATABEND_WAREHOUSE_HEADER));
        body.set(new String(readAllBytes(exchange), StandardCharsets.UTF_8));

        byte[] response = "ok".getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(200, response.length);
        exchange.getResponseBody().write(response);
    }

    private static byte[] readAllBytes(HttpExchange exchange) throws IOException {
        byte[] buffer = new byte[1024];
        int n;
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        while ((n = exchange.getRequestBody().read(buffer)) != -1) {
            output.write(buffer, 0, n);
        }
        return output.toByteArray();
    }
}
