package com.databend.jdbc;

import static com.databend.jdbc.StatementType.QUERY;

import java.util.List;
import java.util.Optional;

import org.apache.commons.lang3.tuple.Pair;


import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * A query statement is a statement that returns data (Typically starts with
 * SELECT, SHOW, etc)
 */
@Getter
@EqualsAndHashCode(callSuper = true)
public class QueryRawStatement extends RawStatement {

    private final String database;

    private final String table;

    public QueryRawStatement(String sql, String cleanSql, List<ParamMarker> paramPositions) {
        super(sql, cleanSql, paramPositions);
        Pair<Optional<String>, Optional<String>> databaseAndTablePair = StatementUtil
                .extractDbNameAndTableNamePairFromCleanQuery(this.getCleanSql());
        this.database = databaseAndTablePair.getLeft().orElse(null);
        this.table = databaseAndTablePair.getRight().orElse(null);
    }

    @Override
    public StatementType getStatementType() {
        return QUERY;
    }

}
