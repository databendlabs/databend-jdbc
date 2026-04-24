package com.databend.jdbc.internal.query;

import com.databend.jdbc.internal.data.DatabendRawType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static com.google.common.base.MoreObjects.toStringHelper;

public class QueryRowField {
    private final String name;
    private final DatabendRawType dataType;

    @JsonCreator
    public QueryRowField(
            @JsonProperty("name") String name,
            @JsonProperty("type") DatabendRawType dataType) {
        this.name = name;
        this.dataType = dataType;
    }

    @JsonProperty("name")
    public String getName() {
        return name;
    }

    @JsonProperty("type")
    public DatabendRawType getDataType() {
        return dataType;
    }

    @Override
    public String toString() {
        return toStringHelper(this).add("name", name).add("type", dataType).toString();
    }
}
