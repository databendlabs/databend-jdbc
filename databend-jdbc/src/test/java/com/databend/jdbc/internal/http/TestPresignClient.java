package com.databend.jdbc.internal.http;

import com.sun.net.httpserver.HttpServer;
import okhttp3.Headers;
import okhttp3.OkHttpClient;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Test(timeOut = 10000)
public class TestPresignClient {
    @Test(groups = {"UNIT"})
    public void testSetupTimeoutsAppliesAllTimeouts() {
        OkHttpClient.Builder builder = new OkHttpClient.Builder();
        OkHttpUtils.setupTimeouts(builder, 7, TimeUnit.SECONDS);
        OkHttpClient client = builder.build();

        Assert.assertEquals(client.connectTimeoutMillis(), 7000);
        Assert.assertEquals(client.readTimeoutMillis(), 7000);
        Assert.assertEquals(client.writeTimeoutMillis(), 7000);
    }

    @Test(groups = {"UNIT"})
    public void testTokenAuthRejectsControlCharacters() {
        Assert.expectThrows(IllegalArgumentException.class, () -> OkHttpUtils.tokenAuth("bad\ntoken"));
    }

    @Test(groups = {"UNIT"})
    public void testPresignDownloadRetriesServiceUnavailable() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/download", exchange -> {
            try {
                int attempt = attempts.incrementAndGet();
                if (attempt < 3) {
                    exchange.sendResponseHeaders(503, -1);
                    return;
                }
                byte[] payload = "hello".getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(200, payload.length);
                exchange.getResponseBody().write(payload);
            }
            finally {
                exchange.close();
            }
        });
        server.start();

        Path file = Files.createTempFile("databend-presign-", ".txt");
        try {
            PresignClient client = new PresignClient();
            client.presignDownload(file.toString(), emptyHeaders(), serverUrl(server, "/download"));

            Assert.assertEquals(new String(Files.readAllBytes(file), StandardCharsets.UTF_8), "hello");
            Assert.assertEquals(attempts.get(), 3);
        }
        finally {
            Files.deleteIfExists(file);
            server.stop(0);
        }
    }

    @Test(groups = {"UNIT"})
    public void testPresignDownloadRetriesBadGateway() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/download-502", exchange -> {
            try {
                int attempt = attempts.incrementAndGet();
                if (attempt < 3) {
                    exchange.sendResponseHeaders(502, -1);
                    return;
                }
                byte[] payload = "hello".getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(200, payload.length);
                exchange.getResponseBody().write(payload);
            }
            finally {
                exchange.close();
            }
        });
        server.start();

        Path file = Files.createTempFile("databend-presign-502-", ".txt");
        try {
            PresignClient client = new PresignClient();
            client.presignDownload(file.toString(), emptyHeaders(), serverUrl(server, "/download-502"));

            Assert.assertEquals(new String(Files.readAllBytes(file), StandardCharsets.UTF_8), "hello");
            Assert.assertEquals(attempts.get(), 3);
        }
        finally {
            Files.deleteIfExists(file);
            server.stop(0);
        }
    }

    @Test(groups = {"UNIT"})
    public void testPresignDownloadStreamPropagatesUnauthorizedFailure() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/download-stream", exchange -> {
            try {
                attempts.incrementAndGet();
                exchange.sendResponseHeaders(401, -1);
            }
            finally {
                exchange.close();
            }
        });
        server.start();

        try {
            PresignClient client = new PresignClient();

            IOException exception = Assert.expectThrows(IOException.class, () ->
                    client.presignDownloadStream(emptyHeaders(), serverUrl(server, "/download-stream")));

            Assert.assertTrue(exception instanceof NonRetryableHttpStatusException, exception.getClass().getName());
            Assert.assertTrue(exception.getMessage().contains("Presign request failed"), exception.getMessage());
            Assert.assertTrue(exception.getMessage().contains("Unauthorized user"), exception.getMessage());
            Assert.assertEquals(attempts.get(), 1);
        }
        finally {
            server.stop(0);
        }
    }

    @Test(groups = {"UNIT"})
    public void testPresignDownloadFailsFastOnUnexpectedRedirectStatus() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/download-300", exchange -> {
            try {
                attempts.incrementAndGet();
                byte[] payload = "multiple choices".getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(300, payload.length);
                exchange.getResponseBody().write(payload);
            }
            finally {
                exchange.close();
            }
        });
        server.start();

        try {
            PresignClient client = new PresignClient();

            IOException exception = Assert.expectThrows(IOException.class, () ->
                    client.presignDownloadStream(emptyHeaders(), serverUrl(server, "/download-300")));

            Assert.assertTrue(exception instanceof NonRetryableHttpStatusException, exception.getClass().getName());
            Assert.assertTrue(exception.getMessage().contains("unexpected response"), exception.getMessage());
            Assert.assertTrue(exception.getMessage().contains("300"), exception.getMessage());
            Assert.assertEquals(attempts.get(), 1);
        }
        finally {
            server.stop(0);
        }
    }

    @Test(groups = {"UNIT"})
    public void testPresignUploadUnauthorizedFailsWithoutRetry() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/upload", exchange -> {
            try {
                attempts.incrementAndGet();
                exchange.sendResponseHeaders(401, -1);
            }
            finally {
                exchange.close();
            }
        });
        server.start();

        try {
            PresignClient client = new PresignClient();

            IOException exception = Assert.expectThrows(IOException.class, () -> client.presignUpload(
                    null,
                    new java.io.ByteArrayInputStream("abc".getBytes(StandardCharsets.UTF_8)),
                    emptyHeaders(),
                    serverUrl(server, "/upload"),
                    3,
                    true));

            Assert.assertTrue(exception instanceof NonRetryableHttpStatusException, exception.getClass().getName());
            Assert.assertTrue(exception.getMessage().contains("Presign request failed"), exception.getMessage());
            Assert.assertTrue(exception.getMessage().contains("Unauthorized user"), exception.getMessage());
            Assert.assertEquals(attempts.get(), 1);
        }
        finally {
            server.stop(0);
        }
    }

    @Test(groups = {"UNIT"})
    public void testPresignUploadConfigurationErrorFailsWithoutRetry() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/upload-400", exchange -> {
            try {
                attempts.incrementAndGet();
                exchange.sendResponseHeaders(400, -1);
            }
            finally {
                exchange.close();
            }
        });
        server.start();

        try {
            PresignClient client = new PresignClient();

            IOException exception = Assert.expectThrows(IOException.class, () -> client.presignUpload(
                    null,
                    new java.io.ByteArrayInputStream("abc".getBytes(StandardCharsets.UTF_8)),
                    emptyHeaders(),
                    serverUrl(server, "/upload-400"),
                    3,
                    true));

            Assert.assertTrue(exception instanceof NonRetryableHttpStatusException, exception.getClass().getName());
            Assert.assertTrue(exception.getMessage().contains("configuration error"), exception.getMessage());
            Assert.assertEquals(attempts.get(), 1);
        }
        finally {
            server.stop(0);
        }
    }

    @Test(groups = {"UNIT"})
    public void testPresignUploadServiceUnavailableFailsAsNotReplayable() throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/upload-503", exchange -> {
            try {
                attempts.incrementAndGet();
                exchange.sendResponseHeaders(503, -1);
            }
            finally {
                exchange.close();
            }
        });
        server.start();

        try {
            PresignClient client = new PresignClient();

            IOException exception = Assert.expectThrows(IOException.class, () -> client.presignUpload(
                    null,
                    new java.io.ByteArrayInputStream("abc".getBytes(StandardCharsets.UTF_8)),
                    emptyHeaders(),
                    serverUrl(server, "/upload-503"),
                    3,
                    true));

            Assert.assertTrue(exception.getMessage().contains("service unavailable"), exception.getMessage());
            Assert.assertTrue(exception instanceof RetryableHttpStatusException, exception.getClass().getName());
            Assert.assertEquals(attempts.get(), 1);
        }
        finally {
            server.stop(0);
        }
    }

    private static Headers emptyHeaders() {
        return new Headers.Builder().build();
    }

    private static String serverUrl(HttpServer server, String path) {
        return "http://127.0.0.1:" + server.getAddress().getPort() + path;
    }
}
