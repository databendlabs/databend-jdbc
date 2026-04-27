package com.databend.jdbc.internal.http;

import com.fasterxml.jackson.core.JsonProcessingException;
import okhttp3.Headers;
import okhttp3.MediaType;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class JsonResponse<T> {
    private final int statusCode;
    private final String statusMessage;
    private final Headers headers;
    private final boolean hasValue;
    private final T value;
    private final IllegalArgumentException exception;

    private JsonResponse(int statusCode, String statusMessage, Headers headers) {
        this.statusCode = statusCode;
        this.statusMessage = statusMessage;
        this.headers = requireNonNull(headers, "headers is null");
        this.hasValue = false;
        this.value = null;
        this.exception = null;
    }

    private JsonResponse(int statusCode, String statusMessage, Headers headers, @Nullable T value, @Nullable IllegalArgumentException exception) {
        this.statusCode = statusCode;
        this.statusMessage = statusMessage;
        this.headers = requireNonNull(headers, "headers is null");
        this.value = value;
        this.exception = exception;
        this.hasValue = (exception == null);
    }

    public static <T> JsonResponse<T> decode(JsonCodec<T> codec, HttpRetryPolicy.ResponseWithBody responseWithBody) {
        String body = new String(responseWithBody.body, StandardCharsets.UTF_8);
        if (isJson(responseWithBody.contentType)) {
            try {
                T value = codec.fromJson(body);
                return new JsonResponse<>(responseWithBody.statusCode, responseWithBody.statusMessage, responseWithBody.headers, value, null);
            } catch (JsonProcessingException e) {
                String message = format("Unable to create %s from JSON response:\n[%s]", codec.getType(), body);
                throw new IllegalArgumentException(message, e);
            }
        }
        return new JsonResponse<>(responseWithBody.statusCode, responseWithBody.statusMessage, responseWithBody.headers);
    }

    private static boolean isJson(MediaType type) {
        return (type != null) && "application".equals(type.type()) && "json".equals(type.subtype());
    }

    public int getStatusCode() {
        return statusCode;
    }

    public Headers getHeaders() {
        return headers;
    }

    public boolean hasValue() {
        return hasValue;
    }

    public T getValue() {
        if (!hasValue) {
            throw new IllegalStateException("Response does not contain a JSON value, please retry", exception);
        }
        return value;
    }

    @Nullable
    public IllegalArgumentException getException() {
        return exception;
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("statusCode", statusCode)
                .add("statusMessage", statusMessage)
                .add("headers", headers.toMultimap())
                .add("hasValue", hasValue)
                .add("value", value)
                .omitNullValues()
                .toString();
    }
}
