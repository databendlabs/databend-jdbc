package com.databend.jdbc;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;

import static com.databend.jdbc.StatementType.PARAM_SETTING;

/**
 * A Set param statement is a special statement that sets a parameter internally
 * (this type of statement starts with SET)
 */
@Getter
@EqualsAndHashCode(callSuper = true)
class SetParamRawStatement extends RawStatement {

    private final Pair<String, String> additionalProperty;

    public SetParamRawStatement(String sql, String cleanSql, List<ParamMarker> paramPositions,
                                Pair<String, String> additionalProperty) {
        super(sql, cleanSql, paramPositions);
        this.additionalProperty = additionalProperty;
    }

    @Override
    public StatementType getStatementType() {
        return PARAM_SETTING;
    }

}
