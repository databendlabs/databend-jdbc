package com.databend.jdbc.internal.binding;

import org.apache.commons.lang3.StringUtils;

import java.util.Collection;
import java.util.List;

public class RawStatementWrapper {

    private final List<RawStatement> subStatements;

    private final long totalParams;

    public RawStatementWrapper(List<RawStatement> subStatements) {
        this.subStatements = subStatements;
        this.totalParams = subStatements.stream().map(RawStatement::getParamMarkers).mapToLong(Collection::size).sum();
    }

    @Override
    public String toString() {
        return "SqlQueryWrapper{" + "subQueries=" + StringUtils.join(subStatements, "|") + ", totalParams="
                + totalParams + '}';
    }

    public List<RawStatement> getSubStatements() {
        return subStatements;
    }

    public long getTotalParams() {
        return totalParams;
    }
}
