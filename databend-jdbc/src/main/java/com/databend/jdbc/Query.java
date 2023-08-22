package com.databend.jdbc;

import lombok.Builder;
import lombok.Value;
import org.apache.commons.lang3.StringUtils;

import java.util.Iterator;
import java.util.List;

/**
 * Represents a SQL query that can be sent to Databend
 */
@Builder
@Value
public class Query {
    String select;
    String from;
    String innerJoin;
    String orderBy;
    List<String> conditions;

    /**
     * Parse the object to a SQL query that can be sent to Firebolt
     *
     * @return SQL query that can be sent to Firebolt
     */
    public String toSql() {
        StringBuilder query = new StringBuilder();
        if (StringUtils.isBlank(select)) {
            throw new IllegalStateException("Cannot create query: SELECT cannot be blank");
        }
        if (StringUtils.isBlank(from)) {
            throw new IllegalStateException("Cannot create query: FROM cannot be blank");
        }

        query.append("SELECT ").append(select);
        query.append(" FROM ").append(from);
        if (StringUtils.isNotBlank(innerJoin)) {
            query.append(" JOIN ").append(innerJoin);
        }
        query.append(getConditionsPart());
        if (StringUtils.isNotBlank(orderBy)) {
            query.append(" order by ").append(orderBy);
        }
        return query.toString();
    }

    private String getConditionsPart() {
        StringBuilder agg = new StringBuilder();
        Iterator<String> iter = conditions.iterator();
        if (iter.hasNext()) {
            agg.append(" WHERE ");
        }
        if (iter.hasNext()) {
            String entry = iter.next();
            agg.append(entry);
        }
        while (iter.hasNext()) {
            String entry = iter.next();
            agg.append(" AND ").append(entry);
        }
        return agg.toString();
    }
}

