package com.databend.jdbc.internal.http;

import com.databend.jdbc.internal.error.CloudErrors;
import okhttp3.Headers;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class HttpRetryPolicy {
    private static final Logger logger = Logger.getLogger(HttpRetryPolicy.class.getPackage().getName());

    public static class ResponseWithBody {
        public final int statusCode;
        public final String statusMessage;
        public final Headers headers;
        public final MediaType contentType;
        public final byte[] body;

        public ResponseWithBody(Response response, byte[] body) {
            this.statusCode = response.code();
            this.statusMessage = response.message();
            this.headers = response.headers();
            this.contentType = response.body().contentType();
            this.body = body;
        }

        public String bodyString() {
            return new String(body, StandardCharsets.UTF_8);
        }
    }

    private static final Random RANDOM = new Random();
    private static final int MAX_ATTEMPTS = 3;
    private static final long INITIAL_INTERVAL = 1000;
    private static final double MULTIPLIER = 4.0;
    private static final long MAX_INTERVAL = 30000;
    private static final List<String> ERROR_KEYWORDS = Arrays.asList(
            "unexpected end of stream",
            "timeout",
            "connection refused"
    );

    private final boolean ignore404;
    private final boolean retry503;

    public HttpRetryPolicy(boolean ignore404, boolean retry503) {
        this.ignore404 = ignore404;
        this.retry503 = retry503;
    }

    public boolean shouldIgnore(int code) {
        return ignore404 && code == 404;
    }

    public boolean shouldRetry(int code, String body) {
        if (retry503 && (code == 502 || code == 503)) {
            return true;
        }
        CloudErrors errors = CloudErrors.tryParse(body);
        return errors != null && errors.tryGetErrorKind().canRetry();
    }

    public static boolean isRetryableIOException(IOException e) {
        if (e instanceof SocketTimeoutException) {
            return true;
        }
        if (e.getCause() instanceof ConnectException) {
            return true;
        }
        String msg = e.getMessage();
        if (msg == null) {
            return false;
        }
        return ERROR_KEYWORDS.stream().anyMatch(msg::contains);
    }

    public boolean shouldRetry(IOException e) {
        return isRetryableIOException(e);
    }

    private static long calculateBackoffInterval(int attempts) {
        long baseInterval = (long) (INITIAL_INTERVAL * Math.pow(MULTIPLIER, attempts - 2));
        baseInterval = Math.min(baseInterval, MAX_INTERVAL);
        double jitter = 1.0 - RANDOM.nextDouble() * 0.3;
        return (long) (baseInterval * jitter);
    }

    public ResponseWithBody sendRequestWithRetry(OkHttpClient httpClient, Request request) throws SQLException {
        String failReason = null;
        Throwable cause = null;
        int attempts = 1;
        long start = System.currentTimeMillis();
        for (; attempts <= MAX_ATTEMPTS; attempts++) {
            if (attempts > 1) {
                try {
                    long interval = calculateBackoffInterval(attempts);
                    logger.log(Level.INFO, "Execute attempt #" + attempts + ", after " + interval + "ms");
                    MILLISECONDS.sleep(interval);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new SQLException("Thread Interrupted");
                }
            }
            try (Response response = httpClient.newCall(request).execute()) {
                int code = response.code();
                if (code != 200) {
                    if (shouldIgnore(code)) {
                        return new ResponseWithBody(response, new byte[0]);
                    }
                    String body = response.body().string();
                    if (!shouldRetry(code, body) || attempts == MAX_ATTEMPTS) {
                        failReason = String.format("status_code = %s, body = %s", code, body);
                        break;
                    }
                } else {
                    byte[] body = response.body().bytes();
                    return new ResponseWithBody(response, body);
                }
            } catch (IOException e) {
                failReason = e.getMessage();
                cause = e;
                if (!shouldRetry(e) || attempts == MAX_ATTEMPTS) {
                    break;
                }
            }
        }
        long elapsed = System.currentTimeMillis() - start;
        String msg = String.format("Error accessing %s: %s after %s attempts (totally %s msecs)", request.url(), failReason, attempts, elapsed);
        throw new SQLException(msg, cause);
    }
}
