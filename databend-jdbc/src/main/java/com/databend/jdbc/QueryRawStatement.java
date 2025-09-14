package com.databend.jdbc;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;
import java.util.Optional;

import static com.databend.jdbc.StatementType.QUERY;

/**
 * A query statement is a statement that returns data (Typically starts with
 * SELECT, SHOW, etc)
 */
@Getter
@EqualsAndHashCode(callSuper = true)
class QueryRawStatement extends RawStatement {

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
