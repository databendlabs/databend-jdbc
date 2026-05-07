package com.databend.jdbc.internal.http;

import okhttp3.Headers;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import okio.BufferedSink;
import okio.Okio;
import okio.Source;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class PresignClient {
    private static final String PRESIGN_REQUEST_FAILED = "Presign request failed";

    private static final int MaxRetryAttempts = 20;
    private static final int MAX_ERROR_BODY_LENGTH = 1024;
    private final OkHttpClient client;
    private static final Logger logger = Logger.getLogger(PresignClient.class.getPackage().getName());

    public PresignClient()
    {
        Logger.getLogger(OkHttpClient.class.getName()).setLevel(Level.FINEST);
        this.client = new OkHttpClient.Builder()
                .connectTimeout(600, TimeUnit.SECONDS)
                .writeTimeout(900, TimeUnit.SECONDS)
                .readTimeout(600, TimeUnit.SECONDS)
                .retryOnConnectionFailure(true)
                .protocols(Arrays.asList(Protocol.HTTP_1_1))
                .build();
    }

    private void uploadFromStream(InputStream inputStream, Headers headers, String presignedUrl, long fileSize)
            throws IOException
    {
        logger.fine("Starting upload: size=" + fileSize + " bytes, url=" + presignedUrl);
        long startTime = System.currentTimeMillis();
        try {
            Request r = putRequest(headers, presignedUrl, inputStream, fileSize);
            executeInternal(r, true);
            logger.fine("Upload completed in " + (System.currentTimeMillis() - startTime) + "ms");
        }
        catch (IOException e) {
            logger.severe("Upload failed after " + (System.currentTimeMillis() - startTime) + "ms: " + e.getMessage());
            throw e;
        }
    }

    private ResponseBody executeInternal(Request request, boolean shouldClose)
            throws IOException
    {
        requireNonNull(request, "request is null");
        long start = System.nanoTime();
        long attempts = 0;
        while (true) {
            attempts++;
            Response response = null;
            try {
                response = client.newCall(request).execute();
                if (response.isSuccessful()) {
                    return response.body();
                }
                String responseBody = readErrorBody(response);
                if (response.code() == 401) {
                    throw new NonRetryableHttpStatusException(
                            formatFailureMessage("Unauthorized user: " + response.code() + " " + response.message())
                                    + formatErrorBody(responseBody));
                }
                else if (isRetryablePresignStatus(response.code())) {
                    throw new RetryableHttpStatusException(formatFailureMessage(
                            "service unavailable: " + response.code() + " " + response.message()));
                }
                else if (response.code() >= 400) {
                    throw new NonRetryableHttpStatusException(
                            formatFailureMessage("configuration error: " + response.code() + " " + response.message())
                                    + formatErrorBody(responseBody));
                }
                else {
                    throw new NonRetryableHttpStatusException(
                            formatFailureMessage("unexpected response: " + response.code() + " " + response.message())
                                    + formatErrorBody(responseBody));
                }
            }
            catch (IOException e) {
                if (!HttpRetryPolicy.isRetryableIOException(e)) {
                    throw e;
                }
                if (request.body() != null && request.body().isOneShot()) {
                    logger.warning(formatFailureMessage("retry aborted because request body is not replayable"));
                    throw retryAbortedIOException(e);
                }
                logger.info(format("%s #%s due to: %s", "retry presign request", attempts, e));
                Duration sinceStart = Duration.ofNanos(System.nanoTime() - start);
                if (sinceStart.getSeconds() >= 900 || attempts >= MaxRetryAttempts) {
                    logger.warning(formatFailureMessage("error is: " + e));
                    throw new PresignRequestFailedException(
                            format("%s (attempts: %s, duration: %s)", PRESIGN_REQUEST_FAILED, attempts, sinceStart),
                            e);
                }
                try {
                    MILLISECONDS.sleep(attempts * 100);
                }
                catch (InterruptedException interruptedException) {
                    Thread.currentThread().interrupt();
                    InterruptedIOException exception = new InterruptedIOException("PresignClient thread was interrupted");
                    exception.initCause(interruptedException);
                    throw exception;
                }
            }
            finally {
                if (shouldClose) {
                    try {
                        if (response != null) {
                            response.close();
                        }
                    }
                    catch (Exception e) {
                        // ignore
                    }
                }
            }
        }
    }

    private static String formatFailureMessage(String detail) {
        return PRESIGN_REQUEST_FAILED + ": " + detail;
    }

    private static IOException retryAbortedIOException(Exception cause) {
        if (cause instanceof IOException) {
            return (IOException) cause;
        }
        String message = cause != null && cause.getMessage() != null
                ? cause.getMessage()
                : formatFailureMessage("retry aborted because request body is not replayable");
        return new IOException(message, cause);
    }

    static boolean isRetryablePresignStatus(int code) {
        return HttpRetryPolicy.isRetryableHttpStatus(code) || code == 504;
    }

    private static String readErrorBody(Response response) throws IOException {
        if (response.body() == null) {
            return "";
        }
        return response.body().string();
    }

    private static String formatErrorBody(String responseBody) {
        if (responseBody == null || responseBody.isEmpty()) {
            return "";
        }
        String body = responseBody.length() > MAX_ERROR_BODY_LENGTH
                ? responseBody.substring(0, MAX_ERROR_BODY_LENGTH) + "...(truncated)"
                : responseBody;
        return ", body=" + body;
    }

    public void presignUpload(File srcFile, InputStream inputStream, Headers headers,
            String presignedUrl, long fileSize, boolean uploadFromStream)
            throws IOException
    {
        if (!uploadFromStream) {
            try (InputStream it = Files.newInputStream(srcFile.toPath())) {
                uploadFromStream(it, headers, presignedUrl, fileSize);
            }
        }
        else {
            uploadFromStream(inputStream, headers, presignedUrl, fileSize);
        }
    }

    public void presignDownload(String destFileName, Headers headers, String presignedUrl)
            throws IOException
    {
        Request r = getRequest(headers, presignedUrl);
        try (ResponseBody body = executeInternal(r, false);
                BufferedSink sink = Okio.buffer(Okio.sink(new File(destFileName)))) {
            sink.writeAll(body.source());
        }
    }

    public InputStream presignDownloadStream(Headers headers, String presignedUrl)
            throws IOException
    {
        Request r = getRequest(headers, presignedUrl);
        ResponseBody responseBody = executeInternal(r, false);
        return responseBody.byteStream();
    }

    private Request getRequest(Headers headers, String url)
    {
        return new Request.Builder().headers(headers).url(url).get().build();
    }

    private Request putRequest(Headers headers, String presignedUrl, InputStream inputStream, long fileSize) {
        RequestBody requestBody = new RequestBody() {
            @Override
            public MediaType contentType() {
                return MediaType.parse("application/octet-stream");
            }

            @Override
            public long contentLength() {
                return fileSize;
            }

            @Override
            public boolean isOneShot() {
                return true;
            }

            @Override
            public void writeTo(BufferedSink sink) throws IOException {
                Source source = Okio.source(inputStream);
                sink.writeAll(source);
            }
        };

        return new Request.Builder()
                .url(presignedUrl)
                .put(requestBody)
                .headers(headers)
                .build();
    }
}
