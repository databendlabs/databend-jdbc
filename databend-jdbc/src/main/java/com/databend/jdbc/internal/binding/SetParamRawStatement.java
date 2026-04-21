package com.databend.jdbc.internal.binding;

import org.apache.commons.lang3.tuple.Pair;

import java.util.List;

import static com.databend.jdbc.internal.binding.StatementType.PARAM_SETTING;

/**
 * A Set param statement is a special statement that sets a parameter internally
 * (this type of statement starts with SET)
 */
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

    public Pair<String, String> getAdditionalProperty() {
        return additionalProperty;
    }
}
