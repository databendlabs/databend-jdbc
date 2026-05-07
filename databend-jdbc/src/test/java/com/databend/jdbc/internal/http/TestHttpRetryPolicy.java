package com.databend.jdbc.internal.http;

import com.sun.net.httpserver.HttpServer;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.net.SocketTimeoutException;
import java.net.InetSocketAddress;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;

@Test(timeOut = 10000)
public class TestHttpRetryPolicy {
    @Test(groups = {"UNIT"})
    public void testIgnored404ReturnsEmptyBody() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/missing", exchange -> {
            try {
                attempts.incrementAndGet();
                exchange.getResponseHeaders().add("Content-Type", "application/json");
                exchange.sendResponseHeaders(404, -1);
            }
            finally {
                exchange.close();
            }
        });
        server.start();

        try {
            HttpRetryPolicy retryPolicy = new HttpRetryPolicy(true, false);
            HttpRetryPolicy.ResponseWithBody response = retryPolicy.sendRequestWithRetry(
                    new OkHttpClient(),
                    new Request.Builder().url(serverUrl(server, "/missing")).get().build());

            Assert.assertEquals(response.statusCode, 404);
            Assert.assertEquals(response.body.length, 0);
            Assert.assertEquals(attempts.get(), 1);
        }
        finally {
            server.stop(0);
        }
    }

    @Test(groups = {"UNIT"})
    public void testNonRetryableStatusThrowsSQLExceptionWithBody() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/unauthorized", exchange -> {
            try {
                attempts.incrementAndGet();
                byte[] payload = "{\"error\":\"denied\"}".getBytes(java.nio.charset.StandardCharsets.UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "application/json");
                exchange.sendResponseHeaders(401, payload.length);
                exchange.getResponseBody().write(payload);
            }
            finally {
                exchange.close();
            }
        });
        server.start();

        try {
            HttpRetryPolicy retryPolicy = new HttpRetryPolicy(false, true);
            SQLException exception = Assert.expectThrows(SQLException.class, () -> retryPolicy.sendRequestWithRetry(
                    new OkHttpClient(),
                    new Request.Builder().url(serverUrl(server, "/unauthorized")).get().build()));

            Assert.assertTrue(exception.getMessage().contains("status_code = 401"), exception.getMessage());
            Assert.assertTrue(exception.getMessage().contains("{\"error\":\"denied\"}"), exception.getMessage());
            Assert.assertEquals(attempts.get(), 1);
        }
        finally {
            server.stop(0);
        }
    }

    @Test(groups = {"UNIT"})
    public void testRetryable503EventuallySucceeds() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/temporary", exchange -> {
            try {
                int attempt = attempts.incrementAndGet();
                if (attempt < 3) {
                    byte[] payload = "{\"error\":\"temporary\"}".getBytes(java.nio.charset.StandardCharsets.UTF_8);
                    exchange.getResponseHeaders().add("Content-Type", "application/json");
                    exchange.sendResponseHeaders(503, payload.length);
                    exchange.getResponseBody().write(payload);
                    return;
                }
                byte[] payload = "{\"ok\":true}".getBytes(java.nio.charset.StandardCharsets.UTF_8);
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
            HttpRetryPolicy retryPolicy = new HttpRetryPolicy(false, true);
            HttpRetryPolicy.ResponseWithBody response = retryPolicy.sendRequestWithRetry(
                    new OkHttpClient(),
                    new Request.Builder().url(serverUrl(server, "/temporary")).get().build());

            Assert.assertEquals(response.statusCode, 200);
            Assert.assertEquals(response.bodyString(), "{\"ok\":true}");
            Assert.assertEquals(attempts.get(), 3);
        }
        finally {
            server.stop(0);
        }
    }

    @Test(groups = {"UNIT"})
    public void testRetryable503ExhaustionThrowsSQLException() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/always-503", exchange -> {
            try {
                attempts.incrementAndGet();
                byte[] payload = "{\"error\":\"temporary\"}".getBytes(java.nio.charset.StandardCharsets.UTF_8);
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
            HttpRetryPolicy retryPolicy = new HttpRetryPolicy(false, true);
            SQLException exception = Assert.expectThrows(SQLException.class, () -> retryPolicy.sendRequestWithRetry(
                    new OkHttpClient(),
                    new Request.Builder().url(serverUrl(server, "/always-503")).get().build()));

            Assert.assertTrue(exception.getMessage().contains("status_code = 503"), exception.getMessage());
            Assert.assertEquals(attempts.get(), 3);
        }
        finally {
            server.stop(0);
        }
    }

    @Test(groups = {"UNIT"})
    public void testGatewayTimeoutIsNotRetriedByGenericPolicy() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/gateway-timeout", exchange -> {
            try {
                attempts.incrementAndGet();
                byte[] payload = "{\"error\":\"temporary\"}".getBytes(java.nio.charset.StandardCharsets.UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "application/json");
                exchange.sendResponseHeaders(504, payload.length);
                exchange.getResponseBody().write(payload);
            }
            finally {
                exchange.close();
            }
        });
        server.start();

        try {
            HttpRetryPolicy retryPolicy = new HttpRetryPolicy(false, true);
            SQLException exception = Assert.expectThrows(SQLException.class, () -> retryPolicy.sendRequestWithRetry(
                    new OkHttpClient(),
                    new Request.Builder().url(serverUrl(server, "/gateway-timeout")).get().build()));

            Assert.assertTrue(exception.getMessage().contains("status_code = 504"), exception.getMessage());
            Assert.assertEquals(attempts.get(), 1);
        }
        finally {
            server.stop(0);
        }
    }

    @Test(groups = {"UNIT"})
    public void testSocketTimeoutExceptionIsRetryable() {
        Assert.assertTrue(HttpRetryPolicy.isRetryableIOException(new SocketTimeoutException("timed out")));
    }

    @Test(groups = {"UNIT"})
    public void testRetryableHttpStatusCodes() {
        Assert.assertTrue(HttpRetryPolicy.isRetryableHttpStatus(502));
        Assert.assertTrue(HttpRetryPolicy.isRetryableHttpStatus(503));
        Assert.assertFalse(HttpRetryPolicy.isRetryableHttpStatus(504));
        Assert.assertFalse(HttpRetryPolicy.isRetryableHttpStatus(500));
        Assert.assertFalse(HttpRetryPolicy.isRetryableHttpStatus(505));
    }

    @Test(groups = {"UNIT"})
    public void testRetryableHttpStatusExceptionIsRetryable() {
        Assert.assertTrue(HttpRetryPolicy.isRetryableIOException(
                new RetryableHttpStatusException("service unavailable: 503 Service Unavailable")));
    }

    @Test(groups = {"UNIT"})
    public void testNonRetryableHttpStatusExceptionIsNotRetryable() {
        Assert.assertFalse(HttpRetryPolicy.isRetryableIOException(
                new NonRetryableHttpStatusException("configuration error: 400 Bad Request")));
    }

    private static String serverUrl(HttpServer server, String path) {
        return "http://127.0.0.1:" + server.getAddress().getPort() + path;
    }
}
