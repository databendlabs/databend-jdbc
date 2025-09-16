package com.databend.jdbc;

import lombok.EqualsAndHashCode;

import java.util.List;

import static com.databend.jdbc.StatementType.NON_QUERY;

/**
 * A non query statement is a statement that does not return data (such as
 * INSERT)
 */
@EqualsAndHashCode(callSuper = true)
class NonQueryRawStatement extends RawStatement {

    public NonQueryRawStatement(String sql, String cleanSql, List<ParamMarker> paramPositions) {
        super(sql, cleanSql, paramPositions);
    }

    @Override
    public StatementType getStatementType() {
        return NON_QUERY;
    }
}
