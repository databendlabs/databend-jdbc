package com.databend.jdbc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import okhttp3.OkHttpClient;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.Types;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

public class TestPreparedStatementBytes {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final byte[] BINARY_VALUE = new byte[]{0x00, (byte) 0xff, 0x27, 0x61, 0x5c};

    @Test(groups = {"UNIT"})
    public void testMavenClasspathProvidesJackson() throws Exception {
        Assert.assertEquals(OBJECT_MAPPER.readTree("{\"value\":1}").get("value").asInt(), 1);
    }

    @Test(groups = {"UNIT"})
    public void testSetBytesUsesHexLiteralByDefault() throws Exception {
        String sql = captureSql(null, statement -> statement.setBytes(1, BINARY_VALUE));

        Assert.assertEquals(sql, "select from_hex('00ff27615c')");
    }

    @Test(groups = {"UNIT"})
    public void testSetBytesUsesConfiguredBase64Literal() throws Exception {
        String sql = captureSql("base64", statement -> statement.setBytes(1, BINARY_VALUE));

        Assert.assertEquals(sql, "select from_base64('AP8nYVw=')");
    }

    @Test(groups = {"UNIT"})
    public void testSetBytesSupportsNull() throws Exception {
        String sql = captureSql(null, statement -> statement.setBytes(1, null));

        Assert.assertEquals(sql, "select null");
    }

    @Test(groups = {"UNIT"})
    public void testBinarySettersShareLosslessEncoding() throws Exception {
        String streamSql = captureSql(null, statement -> statement.setBinaryStream(
                1, new ByteArrayInputStream(BINARY_VALUE)));
        String objectSql = captureSql(null, statement -> statement.setObject(1, BINARY_VALUE, Types.BINARY));

        Assert.assertEquals(streamSql, "select from_hex('00ff27615c')");
        Assert.assertEquals(objectSql, "select from_hex('00ff27615c')");
    }

    private static String captureSql(String binaryFormat, StatementBinder binder) throws Exception {
        AtomicReference<String> requestBody = new AtomicReference<>();
        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/v1/session/login", exchange -> sendJson(exchange,
                "{\"version\":\"1.2.100\",\"server_max_arrow_result_version\":2}"));
        server.createContext("/v1/query", exchange -> {
            requestBody.set(new String(readAllBytes(exchange.getRequestBody()), StandardCharsets.UTF_8));
            sendJson(exchange, "{\"id\":\"qid\",\"node_id\":\"node\","
                    + "\"session\":{\"database\":\"default\"},\"schema\":[],\"data\":[]}");
        });
        server.start();

        try {
            Properties properties = new Properties();
            properties.setProperty("presigned_url_disabled", "true");
            if (binaryFormat != null) {
                properties.setProperty("binary_format", binaryFormat);
            }
            DatabendDriverUri uri = DatabendDriverUri.create(
                    "jdbc:databend://root@127.0.0.1:" + server.getAddress().getPort() + "/default",
                    properties);
            try (DatabendConnection connection = new DatabendConnection(uri, new OkHttpClient());
                 PreparedStatement statement = connection.prepareStatement("select ?")) {
                binder.bind(statement);
                statement.execute();
            }
            return extractJsonStringField(requestBody.get(), "sql");
        } finally {
            server.stop(0);
        }
    }

    /**
     * Minimal JSON string-field reader. The test jar is executed against the shaded driver jar,
     * where Jackson is relocated, so tests must not depend on {@code com.fasterxml.jackson}.
     */
    private static String extractJsonStringField(String json, String field) {
        String marker = "\"" + field + "\":\"";
        int start = json.indexOf(marker);
        Assert.assertTrue(start >= 0, "field " + field + " not found in " + json);
        start += marker.length();

        StringBuilder value = new StringBuilder();
        for (int i = start; i < json.length(); i++) {
            char c = json.charAt(i);
            if (c == '"') {
                return value.toString();
            }
            if (c != '\\') {
                value.append(c);
                continue;
            }
            char escaped = json.charAt(++i);
            switch (escaped) {
                case 'n':
                    value.append('\n');
                    break;
                case 'r':
                    value.append('\r');
                    break;
                case 't':
                    value.append('\t');
                    break;
                case 'b':
                    value.append('\b');
                    break;
                case 'f':
                    value.append('\f');
                    break;
                case 'u':
                    value.append((char) Integer.parseInt(json.substring(i + 1, i + 5), 16));
                    i += 4;
                    break;
                default:
                    value.append(escaped);
                    break;
            }
        }
        throw new IllegalArgumentException("unterminated string for field " + field + " in " + json);
    }

    private static byte[] readAllBytes(InputStream inputStream) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        int read;
        while ((read = inputStream.read(buffer)) != -1) {
            output.write(buffer, 0, read);
        }
        return output.toByteArray();
    }

    private static void sendJson(HttpExchange exchange, String body) throws IOException {
        byte[] response = body.getBytes(StandardCharsets.UTF_8);
        try {
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, response.length);
            exchange.getResponseBody().write(response);
        } finally {
            exchange.close();
        }
    }

    private interface StatementBinder {
        void bind(PreparedStatement statement) throws Exception;
    }
}
