/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package com.databend.client;

import com.databend.client.errors.CloudErrors;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

import java.io.IOException;
import java.net.ConnectException;
import java.sql.SQLException;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.util.concurrent.TimeUnit.MILLISECONDS;


public class RetryPolicy {
    private static final Logger logger = Logger.getLogger(RetryPolicy.class.getPackage().getName());

    public static class ResponseWithBody {
        public Response response;
        public String body;

        public ResponseWithBody(Response response, String body) {
            this.response = response;
            this.body = body;
        }
    }
    private final boolean ignore404;
    private final boolean retry503;

    private static final Random RANDOM = new Random();
    private static final int MAX_ATTEMPTS = 5;
    private static final long INITIAL_INTERVAL = 1000; // 1s
    private static final double MULTIPLIER = 4.0;
    private static final long MAX_INTERVAL = 30000; // 20s
    public RetryPolicy(boolean ignore404, boolean retry503) {
        this.ignore404 = ignore404;
        this.retry503 = retry503;
    }

    public boolean shouldIgnore(int code) {
        return ignore404 && code == 404;
    }

    public boolean shouldRetry(int code, String body) {
        if (retry503 && (code == 500 || code == 502 || code == 503))
            return  true;

        CloudErrors errors = CloudErrors.tryParse(body);
        if (errors != null) {
            return errors.tryGetErrorKind().canRetry();
        }
        return false;
    }

    public boolean shouldRetry(IOException e) {
        return  e.getCause() instanceof ConnectException;
    }

    private static long calculateBackoffInterval(int attempts) {
        long baseInterval = (long) (INITIAL_INTERVAL * Math.pow(MULTIPLIER, attempts - 2));
        baseInterval = Math.min(baseInterval, MAX_INTERVAL);
        double jitter = 1.0 - RANDOM.nextDouble() * 0.3;
        return (long) (baseInterval * jitter);
    }

    public RetryPolicy.ResponseWithBody sendRequestWithRetry(OkHttpClient httpClient, Request request) throws SQLException {
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
                } catch (InterruptedException e2) {
                    Thread.currentThread().interrupt();
                    throw new SQLException("Thread Interrupted");
                }
            }
            try (Response response = httpClient.newCall(request).execute()) {
                int code = response.code();
                if (code != 200) {
                    if (this.shouldIgnore(code)) {
                        return new RetryPolicy.ResponseWithBody(response, "");
                    } else {
                        String body = response.body().string();
                        if (!this.shouldRetry(code, body)) {
                            failReason = String.format("status_code = %s, body = %s", code, body);
                            break;
                        }
                    }
                } else {
                    String body = response.body().string();
                    return new RetryPolicy.ResponseWithBody(response, body);
                }
            } catch (IOException e) {
                failReason = e.getMessage();
                cause = e.getCause();
                if (!this.shouldRetry(e) || attempts == MAX_ATTEMPTS) {
                    break;
                }
            }
        }
        long t = System.currentTimeMillis() - start;
        String msg = String.format("Error accessing %s: %s after %s attempts (totally %s msecs)", request.url(), failReason, attempts, t);
        throw new SQLException(msg, cause);
    }
}
