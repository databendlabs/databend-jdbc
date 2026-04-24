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
import java.net.SocketTimeoutException;
import java.nio.file.Files;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class DatabendPresignClientV1
        implements DatabendPresignClient
{

    private static final int MaxRetryAttempts = 20;
    private final OkHttpClient client;
    private static final Logger logger = Logger.getLogger(DatabendPresignClientV1.class.getPackage().getName());

    public DatabendPresignClientV1(OkHttpClient client, String uri)
    {
        Logger.getLogger(OkHttpClient.class.getName()).setLevel(Level.FINEST);
        OkHttpClient.Builder builder = client.newBuilder();
        this.client = builder.
                connectTimeout(600, TimeUnit.SECONDS)
                .writeTimeout(900, TimeUnit.SECONDS)
                .readTimeout(600, TimeUnit.SECONDS)
                .retryOnConnectionFailure(true)
                .protocols(Arrays.asList(Protocol.HTTP_1_1))
                .addInterceptor(chain -> {
                    Request request = chain.request();
                    boolean oneShot = request.body() != null && request.body().isOneShot();
                    int retryCount = 0;
                    Response response = null;
                    while (retryCount < 3) {
                        try {
                            response = chain.proceed(request);
                            if (response.isSuccessful()) {
                                return response;
                            }
                            if (oneShot) {
                                return response;
                            }
                            response.close();
                        }
                        catch (IOException e) {
                            if (retryCount == 2 || oneShot) {
                                throw e;
                            }
                        }
                        retryCount++;

                        long waitTimeMs = (long) (Math.pow(2, retryCount) * 1000);
                        try {
                            TimeUnit.MILLISECONDS.sleep(waitTimeMs);
                        }
                        catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new IOException("Upload interrupted", e);
                        }
                    }
                    return response;
                }).build();
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
        Exception cause = null;
        while (true) {
            if (attempts > 0) {
                logger.info("try to presign upload again: " + attempts);
                Duration sinceStart = Duration.ofNanos(System.nanoTime() - start);
                if (sinceStart.getSeconds() >= 900) {
                    logger.warning("Presign upload failed, error is:" + cause.toString());
                    throw new RuntimeException(format("Error execute presign (attempts: %s, duration: %s)", attempts, sinceStart), cause);
                }
                if (attempts >= MaxRetryAttempts) {
                    logger.warning("Presign upload failed, error is: " + cause.toString());
                    throw new RuntimeException(format("Error execute presign (attempts: %s, duration: %s)", attempts, sinceStart), cause);
                }

                try {
                    MILLISECONDS.sleep(attempts * 100);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("StatementClient thread was interrupted");
                }
            }
            if (attempts > 0 && request.body() != null && request.body().isOneShot()) {
                throw new IOException("Upload failed and request body is not replayable", cause);
            }
            attempts++;
            Response response = null;
            try {
                response = client.newCall(request).execute();
                if (response.isSuccessful()) {
                    return response.body();
                }
                else if (response.code() == 401) {
                    throw new RuntimeException("Error exeucte presign, Unauthorized user: " + response.code() + " " + response.message());
                }
                else if (response.code() >= 503) {
                    cause = new RuntimeException("Error execute presign, service unavailable: " + response.code() + " " + response.message());
                }
                else if (response.code() >= 400) {
                    cause = new RuntimeException("Error execute presign, configuration error: " + response.code() + " " + response.message());
                }
            }
            catch (SocketTimeoutException e) {
                logger.warning("Error execute presign, socket timeout: " + e.getMessage());
                cause = new RuntimeException("Error execute presign, request is " + request.toString() + "socket timeout: " + e.getMessage());
            }
            catch (RuntimeException e) {
                cause = e;
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

    @Override
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

    @Override
    public void presignDownload(String destFileName, Headers headers, String presignedUrl)
    {
        Request r = getRequest(headers, presignedUrl);
        try (ResponseBody body = executeInternal(r, false)) {
            BufferedSink sink = Okio.buffer(Okio.sink(new File(destFileName)));
            sink.writeAll(body.source());
            sink.close();
        }
        catch (IOException e) {
            throw new RuntimeException("presignDownload failed", e);
        }
    }

    @Override
    public InputStream presignDownloadStream(Headers headers, String presignedUrl)
    {
        Request r = getRequest(headers, presignedUrl);
        try {
            ResponseBody responseBody = executeInternal(r, false);
            return responseBody.byteStream();
        }
        catch (IOException e) {
            throw new RuntimeException("presignDownloadStream failed", e);
        }
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
