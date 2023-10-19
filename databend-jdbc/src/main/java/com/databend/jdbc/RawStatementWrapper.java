package com.databend.jdbc;
import java.util.Collection;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import lombok.CustomLog;
import lombok.Value;

@CustomLog
@Value
public class RawStatementWrapper {

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
