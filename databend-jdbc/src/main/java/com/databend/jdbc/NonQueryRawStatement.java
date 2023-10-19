package com.databend.jdbc;

import static com.databend.jdbc.StatementType.NON_QUERY;

import java.util.List;


import lombok.EqualsAndHashCode;

/**
 * A non query statement is a statement that does not return data (such as
 * INSERT)
 */
@EqualsAndHashCode(callSuper = true)
public class NonQueryRawStatement extends RawStatement {

    public NonQueryRawStatement(String sql, String cleanSql, List<ParamMarker> paramPositions) {
        super(sql, cleanSql, paramPositions);
    }

    @Override
    public StatementType getStatementType() {
        return NON_QUERY;
    }
}
