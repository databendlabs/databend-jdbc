package com.databend.jdbc.internal.error;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static com.google.common.base.MoreObjects.toStringHelper;

public class QueryError {
    private final int code;
    private final String message;

    @JsonCreator
    public QueryError(
            @JsonProperty("code") int code,
            @JsonProperty("message") String message) {
        this.code = code;
        this.message = message;
    }

    public static Builder builder() {
        return new Builder();
    }

    @JsonProperty
    public int getCode() {
        return code;
    }

    @JsonProperty
    public String getMessage() {
        return message;
    }

    public boolean notFound() {
        return code == 404;
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("code", code)
                .add("message", message)
                .toString();
    }

    public static final class Builder {
        private int code;
        private String message;

        public Builder setCode(int code) {
            this.code = code;
            return this;
        }

        public Builder setMessage(String message) {
            this.message = message;
            return this;
        }

        public QueryError build() {
            return new QueryError(code, message);
        }
    }
}
