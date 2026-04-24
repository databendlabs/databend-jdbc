package com.databend.jdbc.internal.error;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import static com.google.common.base.MoreObjects.toStringHelper;

public class CloudErrors {
    private final String kind;
    private final String message;

    @JsonCreator
    public CloudErrors(
            @JsonProperty("kind") String kind,
            @JsonProperty("message") String message) {
        this.kind = kind;
        this.message = message;
    }

    public static CloudErrors tryParse(String json) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            return mapper.readValue(json, CloudErrors.class);
        } catch (Exception e) {
            return null;
        }
    }

    @JsonProperty
    public String getKind() {
        return kind;
    }

    @JsonProperty
    public String getMessage() {
        return message;
    }

    public CloudErrorKinds tryGetErrorKind() {
        return CloudErrorKinds.tryGetErrorKind(kind);
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("kind", kind)
                .add("message", message)
                .toString();
    }
}
