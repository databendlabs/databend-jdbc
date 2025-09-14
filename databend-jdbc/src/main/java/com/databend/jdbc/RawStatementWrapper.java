package com.databend.jdbc;

import lombok.CustomLog;
import lombok.Value;
import org.apache.commons.lang3.StringUtils;

import java.util.Collection;
import java.util.List;

@CustomLog
@Value
class RawStatementWrapper {

    List<RawStatement> subStatements;

    long totalParams;

    public RawStatementWrapper(List<RawStatement> subStatements) {
        this.subStatements = subStatements;
        this.totalParams = subStatements.stream().map(RawStatement::getParamMarkers).mapToLong(Collection::size).sum();
    }

    @Override
    public String toString() {
        return "SqlQueryWrapper{" + "subQueries=" + StringUtils.join(subStatements, "|") + ", totalParams="
                + totalParams + '}';
    }

}
