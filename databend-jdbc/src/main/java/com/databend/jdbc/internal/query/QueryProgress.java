package com.databend.jdbc.internal.query;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.math.BigInteger;

import static com.google.common.base.MoreObjects.toStringHelper;

public class QueryProgress {
    private final BigInteger rows;
    private final BigInteger bytes;

    @JsonCreator
    public QueryProgress(
            @JsonProperty("rows") BigInteger rows,
            @JsonProperty("bytes") BigInteger bytes) {
        this.rows = rows;
        this.bytes = bytes;
    }

    @JsonProperty
    public BigInteger getRows() {
        return rows;
    }

    @JsonProperty
    public BigInteger getBytes() {
        return bytes;
    }

    @Override
    public String toString() {
        return toStringHelper(this).add("rows", rows).add("bytes", bytes).toString();
    }
}
