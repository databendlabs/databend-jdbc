package com.databend.jdbc.internal.http;

import com.fasterxml.jackson.core.JsonProcessingException;
import okhttp3.Headers;
import okhttp3.MediaType;
import okhttp3.Response;

import javax.annotation.Nullable;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class JsonResponse<T> {
    private final int statusCode;
    private final String statusMessage;
    private final Headers headers;
    @Nullable
    private final String responseBody;
    private final boolean hasValue;
    private final T value;
    private final IllegalArgumentException exception;

    private JsonResponse(int statusCode, String statusMessage, Headers headers, String responseBody) {
        this.statusCode = statusCode;
        this.statusMessage = statusMessage;
        this.headers = requireNonNull(headers, "headers is null");
        this.responseBody = requireNonNull(responseBody, "responseBody is null");
        this.hasValue = false;
        this.value = null;
        this.exception = null;
    }

    private JsonResponse(int statusCode, String statusMessage, Headers headers, @Nullable String responseBody, @Nullable T value, @Nullable IllegalArgumentException exception) {
        this.statusCode = statusCode;
        this.statusMessage = statusMessage;
        this.headers = requireNonNull(headers, "headers is null");
        this.responseBody = responseBody;
        this.value = value;
        this.exception = exception;
        this.hasValue = (exception == null);
    }

    public static <T> JsonResponse<T> decode(JsonCodec<T> codec, HttpRetryPolicy.ResponseWithBody responseWithBody) {
        Response response = responseWithBody.response;
        String body = responseWithBody.body;
        if (isJson(response.body().contentType())) {
            try {
                T value = codec.fromJson(body);
                return new JsonResponse<>(response.code(), response.message(), response.headers(), body, value, null);
            } catch (JsonProcessingException e) {
                String message = body != null
                        ? format("Unable to create %s from JSON response:\n[%s]", codec.getType(), body)
                        : format("Unable to create %s from JSON response", codec.getType());
                throw new IllegalArgumentException(message, e);
            }
        }
        return new JsonResponse<>(response.code(), response.message(), response.headers(), body);
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
