import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/** Runs against only the shaded driver jar and the JDK, without TestNG or Maven dependencies. */
public final class Java8JsonSmoke {
    public static void main(String[] args) throws Exception {
        System.out.println("Runtime: " + System.getProperty("java.version"));
        Class<?> pages = Class.forName("com.databend.jdbc.internal.query.RestQueryResultPages");
        pages.getDeclaredMethods();
        pages.getDeclaredFields();
        System.out.println("PASS: query class loading and reflection");
        AtomicBoolean unexpectedArrow = new AtomicBoolean();
        AtomicInteger jsonRequests = new AtomicInteger();
        AtomicInteger unexpectedFormats = new AtomicInteger();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/v1/session/login", exchange -> sendJson(exchange,
                "{\"version\":\"1.2.100\",\"server_max_arrow_result_version\":3}"));
        server.createContext("/v1/session/logout", exchange -> sendJson(exchange, "{}"));
        server.createContext("/v1/query", exchange -> {
            String path = exchange.getRequestURI().getPath();
            if ("/v1/query/final".equals(path)) {
                sendJson(exchange, "{}");
                return;
            }
            if (unexpectedArrow.get()) {
                // The runtime guard must reject even an unsolicited Arrow response before decoding.
                exchange.getResponseHeaders().set("Content-Type", "application/vnd.apache.arrow.stream");
                exchange.sendResponseHeaders(200, 1);
                exchange.getResponseBody().write(0);
                exchange.close();
                return;
            }
            if (!"application/json".equals(exchange.getRequestHeaders().getFirst("Accept"))) {
                unexpectedFormats.incrementAndGet();
            }
            jsonRequests.incrementAndGet();
            boolean next = "/v1/query/next".equals(path);
            sendJson(exchange, page(next ? "second" : "first", next));
        });
        server.start();
        try {
            String url = "jdbc:databend://127.0.0.1:" + server.getAddress().getPort() + "/default";
            Properties properties = new Properties();
            properties.setProperty("user", "root");
            properties.setProperty("presigned_url_disabled", "true");
            // Exercise service-provider discovery, not just Class.forName(driver).
            try (Connection connection = DriverManager.getConnection(url, properties);
                 Statement statement = connection.createStatement();
                 ResultSet results = statement.executeQuery("select 'first'")) {
                checkRows(results);
            }
            System.out.println("PASS: default JSON connection, query, metadata, pagination and close");
            try (Connection connection = DriverManager.getConnection(url + "?query_result_format=json", properties);
                 PreparedStatement statement = connection.prepareStatement("select ?")) {
                statement.setString(1, "first");
                try (ResultSet results = statement.executeQuery()) {
                    checkRows(results);
                }
            }
            require(jsonRequests.get() == 4, "Expected two pages per query, got " + jsonRequests.get());
            require(unexpectedFormats.get() == 0, "Driver did not request JSON");
            System.out.println("PASS: explicit JSON prepared statement and pagination");
            if ("1.8".equals(System.getProperty("java.specification.version"))) {
                expectArrowRuntimeError(url + "?query_result_format=arrow", properties);
                require(jsonRequests.get() == 4, "Arrow request was sent on Java 8");
                unexpectedArrow.set(true);
                expectArrowRuntimeError(url, properties);
                System.out.println("PASS: requested and unsolicited Arrow produce actionable SQLExceptions on Java 8");
            }
        } finally {
            server.stop(0);
        }
    }

    private static void expectArrowRuntimeError(String url, Properties properties) throws Exception {
        try (Connection connection = DriverManager.getConnection(url, properties);
             Statement statement = connection.createStatement()) {
            try (ResultSet ignored = statement.executeQuery("select 1")) {
                throw new AssertionError("Arrow should be rejected on Java 8");
            }
        } catch (SQLException e) {
            Throwable cause = e;
            while (cause != null) {
                String message = cause.getMessage();
                if (message != null && message.contains("Arrow result format requires Java 11 or newer")
                        && message.contains("query_result_format=json")) {
                    return;
                }
                cause = cause.getCause();
            }
            throw new AssertionError("Expected an actionable Arrow runtime error", e);
        }
    }

    private static void checkRows(ResultSet results) throws Exception {
        require(results.getMetaData().getColumnCount() == 1, "Expected one column");
        require("c1".equals(results.getMetaData().getColumnLabel(1)), "Unexpected column label");
        require(results.next(), "Missing first row");
        require("first".equals(results.getString(1)), "Wrong first row");
        require(results.next(), "Missing second-page row");
        require("second".equals(results.getString(1)), "Wrong second-page row");
        require(!results.next(), "Unexpected extra row");
    }

    private static String page(String value, boolean last) {
        return "{\"id\":\"java8-smoke\",\"node_id\":\"node\","
                + "\"session\":{\"database\":\"default\"},"
                + "\"schema\":[{\"name\":\"c1\",\"type\":\"String\"}],"
                + "\"data\":[[\"" + value + "\"]],\"state\":\"Succeeded\","
                + "\"error\":null,\"stats\":null,\"affect\":null,\"result_timeout_secs\":30,"
                + "\"stats_uri\":null,\"final_uri\":\"/v1/query/final\","
                + "\"next_uri\":" + (last ? "null" : "\"/v1/query/next\"") + ",\"kill_uri\":null}";
    }

    private static void sendJson(HttpExchange exchange, String body) throws IOException {
        try {
            byte[] buffer = new byte[1024];
            while (exchange.getRequestBody().read(buffer) != -1) {
                // Consume request bodies before responding so connections can be reused.
            }
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, bytes.length);
            exchange.getResponseBody().write(bytes);
        } finally {
            exchange.close();
        }
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }
}
