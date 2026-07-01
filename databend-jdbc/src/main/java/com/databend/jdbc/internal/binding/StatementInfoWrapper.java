package com.databend.jdbc.internal.binding;

import lombok.NonNull;

import java.util.UUID;

/**
 * This represents a statement that is ready to be sent to Databend.
 */
public class StatementInfoWrapper {
    private String sql;
    private String id;
    private StatementType type;
    private RawStatement initialStatement;

    public StatementInfoWrapper(String sql, String id, StatementType type, RawStatement initialStatement) {
        this.sql = sql;
        this.id = id;
        this.type = type;
        this.initialStatement = initialStatement;
    }

    /**
     * Creates a StatementInfoWrapper from the {@link RawStatement}.
     *
     * @param rawStatement the raw statement
     * @return the statement that will be sent to the server
     */
    public static StatementInfoWrapper of(@NonNull RawStatement rawStatement) {
        return of(rawStatement, UUID.randomUUID().toString().replace("-", ""));
    }

    /**
     * Creates a StatementInfoWrapper from the {@link RawStatement}.
     *
     * @param rawStatement the raw statement
     * @param id the id of the statement to execute
     * @return the statement that will be sent to the server
     */
    public static StatementInfoWrapper of(@NonNull RawStatement rawStatement, String id) {
        return new StatementInfoWrapper(rawStatement.getSql(), id, rawStatement.getStatementType(), rawStatement);
    }

    public String getSql() {
        return sql;
    }

    public String getId() {
        return id;
    }

    public StatementType getType() {
        return type;
    }

    public RawStatement getInitialStatement() {
        return initialStatement;
    }
}
