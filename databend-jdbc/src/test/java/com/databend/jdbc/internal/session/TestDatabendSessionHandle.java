package com.databend.jdbc.internal.session;

import com.databend.jdbc.DriverInfo;
import com.databend.jdbc.internal.QueryResultFormat;
import com.databend.jdbc.internal.exception.DatabendPresignException;
import com.databend.jdbc.internal.exception.DatabendQueryException;
import com.databend.jdbc.internal.exception.DatabendSessionException;
import com.databend.jdbc.internal.exception.DatabendStageUploadException;
import com.databend.jdbc.internal.exception.DatabendStreamingLoadException;
import com.databend.jdbc.internal.query.QueryResultPages;
import okhttp3.Interceptor;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
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
    public void testUploadStreamStageUploadUnauthorizedRaisesSQLExceptionWithStageUploadCause() throws Exception {
        AtomicReference<Integer> attempts = new AtomicReference<>(0);
        OkHttpClient client = new OkHttpClient.Builder()
                .addInterceptor(userAgentInterceptor(DriverInfo.USER_AGENT_VALUE))
                .addInterceptor((Interceptor) chain -> {
                    Request request = chain.request();
                    if ("/v1/upload_to_stage".equals(request.url().encodedPath())) {
                        attempts.set(attempts.get() + 1);
                        return new Response.Builder()
                                .request(request)
                                .protocol(Protocol.HTTP_1_1)
                                .code(401)
                                .message("Unauthorized")
                                .body(ResponseBody.create(MediaType.parse("text/plain"), "denied"))
                                .build();
                    }
                    return chain.proceed(request);
                })
                .build();

        DatabendSessionHandle handle = new DatabendSessionHandle(
                client,
                SessionHandleConfig.builder()
                        .setBaseUri(URI.create("http://127.0.0.1:1"))
                        .setInitialSession(SessionState.createDefault())
                        .setQueryTimeoutSecs(30)
                        .setConnectionTimeoutSecs(30)
                        .setSocketTimeoutSecs(60)
                        .setWaitTimeSecs(10)
                        .setMaxRowsInBuffer(1000)
                        .setMaxRowsPerPage(1000)
                        .build(),
                null);
        handle.initializePresign("off", false);

        SQLException exception = Assert.expectThrows(SQLException.class, () ->
                handle.uploadStream("stage", "dir", new ByteArrayInputStream("a".getBytes(StandardCharsets.UTF_8)),
                        "f.txt", 1, false));

        Assert.assertTrue(exception.getMessage().contains("Failed to upload stream"), exception.getMessage());
        Assert.assertTrue(exception.getCause() instanceof DatabendStageUploadException, String.valueOf(exception.getCause()));
        Assert.assertTrue(exception.getCause().getMessage().contains("Unauthorized user"),
                exception.getCause().getMessage());
        Assert.assertEquals(attempts.get().intValue(), 1);
    }

    @Test(groups = {"UNIT"})
    public void testUploadStreamStageUploadConfigurationErrorRaisesSQLExceptionWithStageUploadCause() throws Exception {
        AtomicReference<Integer> attempts = new AtomicReference<>(0);
        OkHttpClient client = new OkHttpClient.Builder()
                .addInterceptor(userAgentInterceptor(DriverInfo.USER_AGENT_VALUE))
                .addInterceptor((Interceptor) chain -> {
                    Request request = chain.request();
                    if ("/v1/upload_to_stage".equals(request.url().encodedPath())) {
                        attempts.set(attempts.get() + 1);
                        return new Response.Builder()
                                .request(request)
                                .protocol(Protocol.HTTP_1_1)
                                .code(400)
                                .message("Bad Request")
                                .body(ResponseBody.create(MediaType.parse("text/plain"), "bad request"))
                                .build();
                    }
                    return chain.proceed(request);
                })
                .build();

        DatabendSessionHandle handle = new DatabendSessionHandle(
                client,
                SessionHandleConfig.builder()
                        .setBaseUri(URI.create("http://127.0.0.1:1"))
                        .setInitialSession(SessionState.createDefault())
                        .setQueryTimeoutSecs(30)
                        .setConnectionTimeoutSecs(30)
                        .setSocketTimeoutSecs(60)
                        .setWaitTimeSecs(10)
                        .setMaxRowsInBuffer(1000)
                        .setMaxRowsPerPage(1000)
                        .build(),
                null);
        handle.initializePresign("off", false);

        SQLException exception = Assert.expectThrows(SQLException.class, () ->
                handle.uploadStream("stage", "dir", new ByteArrayInputStream("a".getBytes(StandardCharsets.UTF_8)),
                        "f.txt", 1, false));

        Assert.assertTrue(exception.getMessage().contains("Failed to upload stream"), exception.getMessage());
        Assert.assertTrue(exception.getCause() instanceof DatabendStageUploadException, String.valueOf(exception.getCause()));
        Assert.assertTrue(exception.getCause().getMessage().contains("configuration error"),
                exception.getCause().getMessage());
        Assert.assertEquals(attempts.get().intValue(), 1);
    }

    @Test(groups = {"UNIT"})
    public void testExecuteStageUploadServiceUnavailableRaisesDatabendStageUploadException() throws Exception {
        AtomicReference<Integer> attempts = new AtomicReference<>(0);
        OkHttpClient client = new OkHttpClient.Builder()
                .addInterceptor(userAgentInterceptor(DriverInfo.USER_AGENT_VALUE))
                .addInterceptor((Interceptor) chain -> {
                    Request request = chain.request();
                    if ("/v1/upload_to_stage".equals(request.url().encodedPath())) {
                        attempts.set(attempts.get() + 1);
                        return new Response.Builder()
                                .request(request)
                                .protocol(Protocol.HTTP_1_1)
                                .code(503)
                                .message("Service Unavailable")
                                .body(ResponseBody.create(MediaType.parse("text/plain"), "temporary"))
                                .build();
                    }
                    return chain.proceed(request);
                })
                .build();

        DatabendSessionHandle handle = new DatabendSessionHandle(
                client,
                SessionHandleConfig.builder()
                        .setBaseUri(URI.create("http://127.0.0.1:1"))
                        .setInitialSession(SessionState.createDefault())
                        .setQueryTimeoutSecs(30)
                        .setConnectionTimeoutSecs(30)
                        .setSocketTimeoutSecs(60)
                        .setWaitTimeSecs(10)
                        .setMaxRowsInBuffer(1000)
                        .setMaxRowsPerPage(1000)
                        .build(),
                null);

        DatabendStageUploadException exception = Assert.expectThrows(DatabendStageUploadException.class, () ->
                invokeExecuteStageUpload(handle, new Request.Builder()
                        .url("http://127.0.0.1:1/v1/upload_to_stage")
                        .put(new RequestBody() {
                            @Override
                            public MediaType contentType() {
                                return null;
                            }

                            @Override
                            public long contentLength() {
                                return 0;
                            }

                            @Override
                            public boolean isOneShot() {
                                return true;
                            }

                            @Override
                            public void writeTo(okio.BufferedSink sink) {
                            }
                        })
                        .build()));

        Assert.assertTrue(exception.getMessage().contains("service unavailable"), exception.getMessage());
        Assert.assertTrue(exception.getCause().getMessage().contains("service unavailable"),
                exception.getCause().getMessage());
        Assert.assertEquals(attempts.get().intValue(), 1);
    }

    @Test(groups = {"UNIT"})
    public void testExecuteStageUploadUnauthorizedRaisesDatabendStageUploadException() throws Exception {
        AtomicReference<Integer> attempts = new AtomicReference<>(0);
        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/v1/upload_to_stage", exchange -> {
            try {
                attempts.set(attempts.get() + 1);
                byte[] payload = "denied".getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(401, payload.length);
                exchange.getResponseBody().write(payload);
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
                            .setQueryTimeoutSecs(30)
                            .setConnectionTimeoutSecs(30)
                            .setSocketTimeoutSecs(60)
                            .setWaitTimeSecs(10)
                            .setMaxRowsInBuffer(1000)
                            .setMaxRowsPerPage(1000)
                            .build(),
                    null);

            DatabendStageUploadException exception = Assert.expectThrows(DatabendStageUploadException.class, () ->
                    invokeExecuteStageUpload(handle, new Request.Builder()
                            .url("http://127.0.0.1:" + server.getAddress().getPort() + "/v1/upload_to_stage")
                            .put(new RequestBody() {
                                @Override
                                public MediaType contentType() {
                                    return null;
                                }

                                @Override
                                public long contentLength() {
                                    return 0;
                                }

                                @Override
                                public boolean isOneShot() {
                                    return true;
                                }

                                @Override
                                public void writeTo(okio.BufferedSink sink) {
                                }
                            })
                            .build()));

            Assert.assertTrue(exception.getMessage().contains("Unauthorized user"), exception.getMessage());
            Assert.assertEquals(attempts.get().intValue(), 1);
        }
        finally {
            server.stop(0);
        }
    }

    @Test(groups = {"UNIT"})
    public void testExecuteStageUploadConfigurationErrorRaisesDatabendStageUploadException() throws Exception {
        AtomicReference<Integer> attempts = new AtomicReference<>(0);
        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/v1/upload_to_stage", exchange -> {
            try {
                attempts.set(attempts.get() + 1);
                byte[] payload = "bad request".getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(400, payload.length);
                exchange.getResponseBody().write(payload);
            }
            finally {
                exchange.close();
            }
        });
        server.start();

        try {
            DatabendSessionHandle handle = createSessionHandle(
                    URI.create("http://127.0.0.1:" + server.getAddress().getPort()));

            DatabendStageUploadException exception = Assert.expectThrows(DatabendStageUploadException.class, () ->
                    invokeExecuteStageUpload(handle, new Request.Builder()
                            .url("http://127.0.0.1:" + server.getAddress().getPort() + "/v1/upload_to_stage")
                            .put(new RequestBody() {
                                @Override
                                public MediaType contentType() {
                                    return null;
                                }

                                @Override
                                public long contentLength() {
                                    return 0;
                                }

                                @Override
                                public boolean isOneShot() {
                                    return true;
                                }

                                @Override
                                public void writeTo(okio.BufferedSink sink) {
                                }
                            })
                            .build()));

            Assert.assertTrue(exception.getMessage().contains("configuration error"), exception.getMessage());
            Assert.assertEquals(attempts.get().intValue(), 1);
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
                byte[] response = "{\"version\":\"1.2.100\",\"server_max_arrow_result_version\":3}".getBytes(StandardCharsets.UTF_8);
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
            Assert.assertTrue(requestBody.get().contains("\"arrow_result_version_max\":3"));
            Assert.assertTrue(requestBody.get().contains("\"arrow_features\":{\"decimal64\":false}"));
        }
        finally {
            server.stop(0);
        }
    }

    @Test(groups = {"UNIT"})
    public void testLoginFallsBackToJsonWhenServerArrowResultVersionIsTooLow() throws Exception {
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

            Assert.assertEquals(accept.get(), "application/json");
            Assert.assertFalse(requestBody.get().contains("\"arrow_result_version_max\""));
            Assert.assertFalse(requestBody.get().contains("\"arrow_features\""));
        }
        finally {
            server.stop(0);
        }
    }

    @Test(groups = {"UNIT"})
    public void testLoginMalformedJsonRaisesDatabendSessionException() throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/v1/session/login", exchange -> {
            try {
                byte[] response = "{\"version\":".getBytes(StandardCharsets.UTF_8);
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
                            .setQueryTimeoutSecs(30)
                            .setConnectionTimeoutSecs(30)
                            .setSocketTimeoutSecs(60)
                            .setWaitTimeSecs(10)
                            .setMaxRowsInBuffer(1000)
                            .setMaxRowsPerPage(1000)
                            .build(),
                    null);

            DatabendSessionException exception = Assert.expectThrows(DatabendSessionException.class, handle::login);
            Assert.assertTrue(exception.getMessage().contains("Failed to decode login response"), exception.getMessage());
            Assert.assertNotNull(exception.getCause());
        }
        finally {
            server.stop(0);
        }
    }

    @Test(groups = {"UNIT"})
    public void testStreamingLoadMalformedJsonRaisesDatabendStreamingLoadException() throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/v1/streaming_load", exchange -> {
            try {
                byte[] response = "{\"stats\":".getBytes(StandardCharsets.UTF_8);
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
                            .setQueryTimeoutSecs(30)
                            .setConnectionTimeoutSecs(30)
                            .setSocketTimeoutSecs(60)
                            .setWaitTimeSecs(10)
                            .setMaxRowsInBuffer(1000)
                            .setMaxRowsPerPage(1000)
                            .build(),
                    null);

            DatabendStreamingLoadException exception = Assert.expectThrows(DatabendStreamingLoadException.class, () ->
                    handle.streamingLoad("insert into t from @_databend_load file_format=(type=csv)",
                            new ByteArrayInputStream("a".getBytes(StandardCharsets.UTF_8)),
                            1));
            Assert.assertTrue(exception.getMessage().contains("Failed to decode streaming load response"), exception.getMessage());
            Assert.assertNotNull(exception.getCause());
        }
        finally {
            server.stop(0);
        }
    }

    @Test(groups = {"UNIT"})
    public void testStreamingLoadRequestEncodingFailureRaisesDatabendStreamingLoadException() throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/v1/streaming_load", exchange -> {
            try {
                exchange.sendResponseHeaders(200, -1);
            }
            finally {
                exchange.close();
            }
        });
        server.start();

        try {
            DatabendSessionHandle handle = createSessionHandle(
                    URI.create("http://127.0.0.1:" + server.getAddress().getPort()));

            DatabendStreamingLoadException exception = Assert.expectThrows(DatabendStreamingLoadException.class, () ->
                    handle.streamingLoad("insert into t from @_databend_load file_format=(type=csv)",
                            new java.io.InputStream() {
                                @Override
                                public int read() throws IOException {
                                    throw new IOException("streaming source failed");
                                }
                            },
                            1));
            Assert.assertTrue(exception.getMessage().contains("Failed to encode streaming load request"), exception.getMessage());
            Assert.assertTrue(exception.getCause() instanceof IOException, String.valueOf(exception.getCause()));
            Assert.assertTrue(exception.getCause().getMessage().contains("streaming source failed"),
                    exception.getCause().getMessage());
        }
        finally {
            server.stop(0);
        }
    }

    @Test(groups = {"UNIT"})
    public void testStreamingLoadServerErrorRemainsSQLException() throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/v1/streaming_load", exchange -> {
            try {
                byte[] response = "{\"error\":{\"code\":1065,\"message\":\"bad load\"}}".getBytes(StandardCharsets.UTF_8);
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
                            .setQueryTimeoutSecs(30)
                            .setConnectionTimeoutSecs(30)
                            .setSocketTimeoutSecs(60)
                            .setWaitTimeSecs(10)
                            .setMaxRowsInBuffer(1000)
                            .setMaxRowsPerPage(1000)
                            .build(),
                    null);

            SQLException exception = Assert.expectThrows(SQLException.class, () ->
                    handle.streamingLoad("insert into t from @_databend_load file_format=(type=csv)",
                            new ByteArrayInputStream("a".getBytes(StandardCharsets.UTF_8)),
                            1));
            Assert.assertTrue(exception.getMessage().contains("streaming load fail: code = 1065"), exception.getMessage());
            Assert.assertEquals(exception.getClass(), SQLException.class);
        }
        finally {
            server.stop(0);
        }
    }

    @Test(groups = {"UNIT"})
    public void testDownloadStreamMalformedPresignResponseRaisesSQLExceptionWithPresignCause() throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/v1/query", exchange -> {
            try {
                byte[] response = invalidPresignQueryResponseMissingHeaders("http://127.0.0.1/download")
                        .getBytes(StandardCharsets.UTF_8);
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
                            .setQueryTimeoutSecs(30)
                            .setConnectionTimeoutSecs(30)
                            .setSocketTimeoutSecs(60)
                            .setWaitTimeSecs(10)
                            .setMaxRowsInBuffer(1000)
                            .setMaxRowsPerPage(1000)
                            .build(),
                    null);

            SQLException exception = Assert.expectThrows(SQLException.class,
                    () -> handle.downloadStream("~", "path/file.txt"));
            Assert.assertTrue(exception.getMessage().contains("Failed to prepare presigned download request"), exception.getMessage());
            Assert.assertTrue(exception.getCause() instanceof DatabendPresignException, String.valueOf(exception.getCause()));
            Assert.assertTrue(exception.getCause().getMessage().contains("Failed to decode presign response"),
                    exception.getCause().getMessage());
        }
        finally {
            server.stop(0);
        }
    }

    @Test(groups = {"UNIT"})
    public void testUploadStreamMalformedPresignResponseRaisesSQLExceptionWithPresignCause() throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/v1/query", exchange -> {
            try {
                byte[] response = invalidPresignQueryResponseMissingHeaders("http://127.0.0.1/upload")
                        .getBytes(StandardCharsets.UTF_8);
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
                            .setQueryTimeoutSecs(30)
                            .setConnectionTimeoutSecs(30)
                            .setSocketTimeoutSecs(60)
                            .setWaitTimeSecs(10)
                            .setMaxRowsInBuffer(1000)
                            .setMaxRowsPerPage(1000)
                            .build(),
                    null);
            handle.initializePresign("on", false);

            SQLException exception = Assert.expectThrows(SQLException.class, () ->
                    handle.uploadStream("~", "dir", new ByteArrayInputStream("a".getBytes(StandardCharsets.UTF_8)),
                            "f.txt", 1, false));
            Assert.assertTrue(exception.getMessage().contains("Failed to prepare presigned upload request"), exception.getMessage());
            Assert.assertTrue(exception.getCause() instanceof DatabendPresignException, String.valueOf(exception.getCause()));
            Assert.assertTrue(exception.getCause().getMessage().contains("Failed to decode presign response"),
                    exception.getCause().getMessage());
        }
        finally {
            server.stop(0);
        }
    }

    @Test(groups = {"UNIT"})
    public void testUploadStreamPresignedUploadFailureRaisesSQLException() throws Exception {
        HttpServer queryServer = HttpServer.create(new InetSocketAddress(0), 0);
        HttpServer uploadServer = HttpServer.create(new InetSocketAddress(0), 0);
        uploadServer.createContext("/upload", exchange -> {
            try {
                exchange.sendResponseHeaders(401, -1);
            }
            finally {
                exchange.close();
            }
        });
        queryServer.createContext("/v1/query", exchange -> {
            try {
                byte[] response = presignQueryResponse("{}",
                        "http://127.0.0.1:" + uploadServer.getAddress().getPort() + "/upload")
                        .getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, response.length);
                exchange.getResponseBody().write(response);
            }
            finally {
                exchange.close();
            }
        });
        uploadServer.start();
        queryServer.start();

        try {
            DatabendSessionHandle handle = createSessionHandle(
                    URI.create("http://127.0.0.1:" + queryServer.getAddress().getPort()));
            handle.initializePresign("on", false);

            SQLException exception = Assert.expectThrows(SQLException.class, () ->
                    handle.uploadStream("~", "dir", new ByteArrayInputStream("a".getBytes(StandardCharsets.UTF_8)),
                            "f.txt", 1, false));
            Assert.assertTrue(exception.getMessage().contains("Failed to upload stream"), exception.getMessage());
            Assert.assertTrue(exception.getCause() instanceof DatabendPresignException, String.valueOf(exception.getCause()));
            Assert.assertTrue(exception.getCause().getMessage().contains("Failed to upload via presigned request"),
                    exception.getCause().getMessage());
            Assert.assertTrue(exception.getCause().getCause().getMessage().contains("Unauthorized user"),
                    exception.getCause().getCause().getMessage());
        }
        finally {
            queryServer.stop(0);
            uploadServer.stop(0);
        }
    }

    @Test(groups = {"UNIT"})
    public void testUploadStreamCompressionFailureRaisesSQLException() {
        DatabendSessionHandle handle = createSessionHandle(URI.create("http://127.0.0.1:1"));

        SQLException exception = Assert.expectThrows(SQLException.class, () ->
                handle.uploadStream("~", "dir", new java.io.InputStream() {
                    @Override
                    public int read() throws IOException {
                        throw new IOException("compress source failed");
                    }
                }, "f.txt", 1, true));

        Assert.assertTrue(exception.getMessage().contains("Failed to upload stream"), exception.getMessage());
        Assert.assertTrue(exception.getCause() instanceof IOException, String.valueOf(exception.getCause()));
        Assert.assertTrue(exception.getCause().getMessage().contains("compress source failed"),
                exception.getCause().getMessage());
    }

    @Test(groups = {"UNIT"})
    public void testUploadStreamCompressedPresignedUploadFailureRaisesSQLException() throws Exception {
        HttpServer queryServer = HttpServer.create(new InetSocketAddress(0), 0);
        HttpServer uploadServer = HttpServer.create(new InetSocketAddress(0), 0);
        uploadServer.createContext("/upload", exchange -> {
            try {
                exchange.sendResponseHeaders(401, -1);
            }
            finally {
                exchange.close();
            }
        });
        queryServer.createContext("/v1/query", exchange -> {
            try {
                byte[] response = presignQueryResponse("{}",
                        "http://127.0.0.1:" + uploadServer.getAddress().getPort() + "/upload")
                        .getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, response.length);
                exchange.getResponseBody().write(response);
            }
            finally {
                exchange.close();
            }
        });
        uploadServer.start();
        queryServer.start();

        try {
            DatabendSessionHandle handle = createSessionHandle(
                    URI.create("http://127.0.0.1:" + queryServer.getAddress().getPort()));
            handle.initializePresign("on", false);

            SQLException exception = Assert.expectThrows(SQLException.class, () ->
                    handle.uploadStream("~", "dir", new ByteArrayInputStream("compress-me".getBytes(StandardCharsets.UTF_8)),
                            "f.txt", "compress-me".getBytes(StandardCharsets.UTF_8).length, true));
            Assert.assertTrue(exception.getMessage().contains("Failed to upload stream"), exception.getMessage());
            Assert.assertTrue(exception.getCause() instanceof DatabendPresignException, String.valueOf(exception.getCause()));
            Assert.assertTrue(exception.getCause().getMessage().contains("Failed to upload via presigned request"),
                    exception.getCause().getMessage());
            Assert.assertTrue(exception.getCause().getCause().getMessage().contains("Unauthorized user"),
                    exception.getCause().getCause().getMessage());
        }
        finally {
            queryServer.stop(0);
            uploadServer.stop(0);
        }
    }

    @Test(groups = {"UNIT"})
    public void testUploadStreamPresignedServiceUnavailableRaisesSQLExceptionWithPresignCause() throws Exception {
        HttpServer queryServer = HttpServer.create(new InetSocketAddress(0), 0);
        HttpServer uploadServer = HttpServer.create(new InetSocketAddress(0), 0);
        AtomicReference<Integer> uploadAttempts = new AtomicReference<>(0);
        uploadServer.createContext("/upload", exchange -> {
            try {
                uploadAttempts.set(uploadAttempts.get() + 1);
                byte[] payload = "temporary".getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(503, payload.length);
                exchange.getResponseBody().write(payload);
            }
            finally {
                exchange.close();
            }
        });
        queryServer.createContext("/v1/query", exchange -> {
            try {
                byte[] response = presignQueryResponse("{}",
                        "http://127.0.0.1:" + uploadServer.getAddress().getPort() + "/upload")
                        .getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, response.length);
                exchange.getResponseBody().write(response);
            }
            finally {
                exchange.close();
            }
        });
        uploadServer.start();
        queryServer.start();

        try {
            DatabendSessionHandle handle = createSessionHandle(
                    URI.create("http://127.0.0.1:" + queryServer.getAddress().getPort()));
            handle.initializePresign("on", false);

            SQLException exception = Assert.expectThrows(SQLException.class, () ->
                    handle.uploadStream("~", "dir", new ByteArrayInputStream("a".getBytes(StandardCharsets.UTF_8)),
                            "f.txt", 1, false));
            Assert.assertTrue(exception.getMessage().contains("Failed to upload stream"), exception.getMessage());
            Assert.assertTrue(exception.getCause() instanceof DatabendPresignException, String.valueOf(exception.getCause()));
            Assert.assertTrue(exception.getCause().getMessage().contains("Failed to upload via presigned request"),
                    exception.getCause().getMessage());
            Assert.assertTrue(exception.getCause().getCause().getMessage().contains("service unavailable"),
                    exception.getCause().getCause().getMessage());
            Assert.assertTrue(exception.getCause().getCause().getCause().getMessage().contains("service unavailable"),
                    exception.getCause().getCause().getCause().getMessage());
            Assert.assertEquals(uploadAttempts.get().intValue(), 1);
        }
        finally {
            queryServer.stop(0);
            uploadServer.stop(0);
        }
    }

    @Test(groups = {"UNIT"})
    public void testDownloadStreamPresignResponseWithoutRowsRaisesSQLExceptionWithPresignCause() throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/v1/query", exchange -> {
            try {
                byte[] response = presignQueryResponseWithoutRows().getBytes(StandardCharsets.UTF_8);
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
            DatabendSessionHandle handle = createSessionHandle(URI.create("http://127.0.0.1:" + server.getAddress().getPort()));

            SQLException exception = Assert.expectThrows(SQLException.class,
                    () -> handle.downloadStream("~", "path/file.txt"));
            Assert.assertTrue(exception.getMessage().contains("Failed to prepare presigned download request"), exception.getMessage());
            Assert.assertTrue(exception.getCause() instanceof DatabendPresignException, String.valueOf(exception.getCause()));
            Assert.assertTrue(exception.getCause().getMessage().contains("no result returned"),
                    exception.getCause().getMessage());
        }
        finally {
            server.stop(0);
        }
    }

    @Test(groups = {"UNIT"})
    public void testDownloadStreamInvalidPresignHeadersRaisesSQLExceptionWithPresignCause() throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/v1/query", exchange -> {
            try {
                byte[] response = presignQueryResponse("not-json", "http://127.0.0.1/download")
                        .getBytes(StandardCharsets.UTF_8);
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
            DatabendSessionHandle handle = createSessionHandle(URI.create("http://127.0.0.1:" + server.getAddress().getPort()));

            SQLException exception = Assert.expectThrows(SQLException.class,
                    () -> handle.downloadStream("~", "path/file.txt"));
            Assert.assertTrue(exception.getMessage().contains("Failed to prepare presigned download request"), exception.getMessage());
            Assert.assertTrue(exception.getCause() instanceof DatabendPresignException, String.valueOf(exception.getCause()));
            Assert.assertTrue(exception.getCause().getMessage().contains("Failed to decode presign response"),
                    exception.getCause().getMessage());
        }
        finally {
            server.stop(0);
        }
    }

    @Test(groups = {"UNIT"})
    public void testDownloadStreamPresignQueryErrorRaisesSQLException() throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/v1/query", exchange -> {
            try {
                byte[] response = presignQueryErrorResponse(1065, "presign failed").getBytes(StandardCharsets.UTF_8);
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
            DatabendSessionHandle handle = createSessionHandle(URI.create("http://127.0.0.1:" + server.getAddress().getPort()));

            SQLException exception = Assert.expectThrows(SQLException.class,
                    () -> handle.downloadStream("~", "path/file.txt"));
            Assert.assertTrue(exception.getMessage().contains("Failed to prepare presigned download request"), exception.getMessage());
            Assert.assertTrue(exception.getCause() instanceof DatabendQueryException, String.valueOf(exception.getCause()));
            Assert.assertTrue(exception.getCause().getMessage().contains("Failed to start query"),
                    exception.getCause().getMessage());
        }
        finally {
            server.stop(0);
        }
    }

    @Test(groups = {"UNIT"})
    public void testDownloadStreamPresignedOpenFailureRaisesSQLException() throws Exception {
        HttpServer queryServer = HttpServer.create(new InetSocketAddress(0), 0);
        HttpServer downloadServer = HttpServer.create(new InetSocketAddress(0), 0);
        downloadServer.createContext("/download", exchange -> {
            try {
                exchange.sendResponseHeaders(401, -1);
            }
            finally {
                exchange.close();
            }
        });
        queryServer.createContext("/v1/query", exchange -> {
            try {
                byte[] response = presignQueryResponse("{}",
                        "http://127.0.0.1:" + downloadServer.getAddress().getPort() + "/download")
                        .getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, response.length);
                exchange.getResponseBody().write(response);
            }
            finally {
                exchange.close();
            }
        });
        downloadServer.start();
        queryServer.start();

        try {
            DatabendSessionHandle handle = createSessionHandle(
                    URI.create("http://127.0.0.1:" + queryServer.getAddress().getPort()));

            SQLException exception = Assert.expectThrows(SQLException.class,
                    () -> handle.downloadStream("~", "path/file.txt"));
            Assert.assertTrue(exception.getMessage().contains("Failed to open presigned download stream"), exception.getMessage());
            Assert.assertTrue(exception.getCause() instanceof DatabendPresignException, String.valueOf(exception.getCause()));
            Assert.assertTrue(exception.getCause().getMessage().contains("Failed to open presigned download stream"),
                    exception.getCause().getMessage());
            Assert.assertTrue(exception.getCause().getCause().getMessage().contains("Unauthorized user"),
                    exception.getCause().getCause().getMessage());
        }
        finally {
            queryServer.stop(0);
            downloadServer.stop(0);
        }
    }

    @Test(groups = {"UNIT"})
    public void testHeartbeatMalformedJsonResponseKeepsQueriesActive() throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/v1/session/heartbeat", exchange -> {
            try {
                byte[] response = "{\"queries_to_remove\":".getBytes(StandardCharsets.UTF_8);
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
            DatabendSessionHandle handle = createSessionHandle(URI.create("http://127.0.0.1:" + server.getAddress().getPort()));
            QueryLiveness queryLiveness = createHeartbeatQuery("query-malformed-json");

            invokeSendHeartbeat(handle, Collections.singletonList(queryLiveness));

            Assert.assertFalse(queryLiveness.stopped);
        }
        finally {
            server.stop(0);
        }
    }

    @Test(groups = {"UNIT"})
    public void testHeartbeatInvalidQueriesToRemoveShapeKeepsQueriesActive() throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/v1/session/heartbeat", exchange -> {
            try {
                byte[] response = "{\"queries_to_remove\":\"query-invalid-shape\"}".getBytes(StandardCharsets.UTF_8);
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
            DatabendSessionHandle handle = createSessionHandle(URI.create("http://127.0.0.1:" + server.getAddress().getPort()));
            QueryLiveness queryLiveness = createHeartbeatQuery("query-invalid-shape");

            invokeSendHeartbeat(handle, Collections.singletonList(queryLiveness));

            Assert.assertFalse(queryLiveness.stopped);
        }
        finally {
            server.stop(0);
        }
    }

    @Test(groups = {"UNIT"})
    public void testHeartbeatHttpFailureKeepsQueriesActive() throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/v1/session/heartbeat", exchange -> {
            try {
                byte[] response = "{\"error\":\"boom\"}".getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "application/json");
                exchange.sendResponseHeaders(500, response.length);
                exchange.getResponseBody().write(response);
            }
            finally {
                exchange.close();
            }
        });
        server.start();

        try {
            DatabendSessionHandle handle = createSessionHandle(URI.create("http://127.0.0.1:" + server.getAddress().getPort()));
            QueryLiveness queryLiveness = createHeartbeatQuery("query-http-failure");

            invokeSendHeartbeat(handle, Collections.singletonList(queryLiveness));

            Assert.assertFalse(queryLiveness.stopped);
        }
        finally {
            server.stop(0);
        }
    }

    private static String presignQueryResponse(String headersJson, String url) {
        return "{"
                + "\"id\":\"qid\","
                + "\"node_id\":\"node\","
                + "\"session\":{\"database\":\"default\"},"
                + "\"schema\":[{\"name\":\"headers\",\"type\":\"String\"},{\"name\":\"url\",\"type\":\"String\"}],"
                + "\"data\":[[" + jsonString(headersJson) + "," + jsonString(url) + "]],"
                + "\"state\":\"Succeeded\","
                + "\"error\":null,"
                + "\"stats\":null,"
                + "\"affect\":null,"
                + "\"result_timeout_secs\":30,"
                + "\"stats_uri\":null,"
                + "\"final_uri\":null,"
                + "\"next_uri\":null,"
                + "\"kill_uri\":null"
                + "}";
    }

    private static String invalidPresignQueryResponseMissingHeaders(String url) {
        return "{"
                + "\"id\":\"qid\","
                + "\"node_id\":\"node\","
                + "\"session\":{\"database\":\"default\"},"
                + "\"schema\":[{\"name\":\"url\",\"type\":\"String\"}],"
                + "\"data\":[[" + jsonString(url) + "]],"
                + "\"state\":\"Succeeded\","
                + "\"error\":null,"
                + "\"stats\":null,"
                + "\"affect\":null,"
                + "\"result_timeout_secs\":30,"
                + "\"stats_uri\":null,"
                + "\"final_uri\":null,"
                + "\"next_uri\":null,"
                + "\"kill_uri\":null"
                + "}";
    }

    private static String presignQueryResponseWithoutRows() {
        return "{"
                + "\"id\":\"qid\","
                + "\"node_id\":\"node\","
                + "\"session\":{\"database\":\"default\"},"
                + "\"schema\":[{\"name\":\"headers\",\"type\":\"String\"},{\"name\":\"url\",\"type\":\"String\"}],"
                + "\"data\":[],"
                + "\"state\":\"Succeeded\","
                + "\"error\":null,"
                + "\"stats\":null,"
                + "\"affect\":null,"
                + "\"result_timeout_secs\":30,"
                + "\"stats_uri\":null,"
                + "\"final_uri\":null,"
                + "\"next_uri\":null,"
                + "\"kill_uri\":null"
                + "}";
    }

    private static String presignQueryErrorResponse(int code, String message) {
        return "{"
                + "\"id\":\"qid\","
                + "\"node_id\":\"node\","
                + "\"session\":{\"database\":\"default\"},"
                + "\"schema\":[],"
                + "\"data\":[],"
                + "\"state\":\"Failed\","
                + "\"error\":{\"code\":" + code + ",\"message\":" + jsonString(message) + "},"
                + "\"stats\":null,"
                + "\"affect\":null,"
                + "\"result_timeout_secs\":30,"
                + "\"stats_uri\":null,"
                + "\"final_uri\":null,"
                + "\"next_uri\":null,"
                + "\"kill_uri\":null"
                + "}";
    }

    private static String jsonString(String value) {
        return "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
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

    private static DatabendSessionHandle createSessionHandle(URI baseUri) {
        return new DatabendSessionHandle(
                new OkHttpClient.Builder()
                        .addInterceptor(userAgentInterceptor(DriverInfo.USER_AGENT_VALUE))
                        .build(),
                SessionHandleConfig.builder()
                        .setBaseUri(baseUri)
                        .setInitialSession(SessionState.createDefault())
                        .setQueryTimeoutSecs(30)
                        .setConnectionTimeoutSecs(30)
                        .setSocketTimeoutSecs(60)
                        .setWaitTimeSecs(10)
                        .setMaxRowsInBuffer(1000)
                        .setMaxRowsPerPage(1000)
                        .build(),
                null);
    }

    private static QueryLiveness createHeartbeatQuery(String queryId) {
        return new QueryLiveness(
                queryId,
                "node",
                new AtomicLong(System.currentTimeMillis() - 20_000),
                30L,
                true);
    }

    private static void invokeSendHeartbeat(DatabendSessionHandle handle, List<QueryLiveness> queryLivenesses) throws Exception {
        Method method = DatabendSessionHandle.class.getDeclaredMethod("sendHeartbeat", List.class);
        method.setAccessible(true);
        method.invoke(handle, queryLivenesses);
    }

    private static void invokeExecuteStageUpload(DatabendSessionHandle handle, Request request) throws Exception {
        Method method = DatabendSessionHandle.class.getDeclaredMethod("executeStageUpload", Request.class);
        method.setAccessible(true);
        try {
            method.invoke(handle, request);
        } catch (java.lang.reflect.InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            if (cause instanceof Exception) {
                throw (Exception) cause;
            }
            throw e;
        }
    }
}
