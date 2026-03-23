package com.databend.jdbc.exception;

import java.sql.SQLException;

public class DatabendSQLException extends SQLException {
    private static final String QUERY_ID_PREFIX = "query_id=";

    public DatabendSQLException() {
        super();
    }

    public DatabendSQLException(String message, String queryId) {
        super(formatMessage(message, queryId), queryId);
    }

    public DatabendSQLException(String message, String queryId, Throwable cause) {
        super(formatMessage(message, queryId), queryId, cause);
    }

    public DatabendSQLException(SQLException exception, String queryId) {
        super(formatMessage(exception.getMessage(), queryId), exception.getSQLState(), exception.getErrorCode(), exception.getCause());
        setStackTrace(exception.getStackTrace());

        SQLException nextException = exception.getNextException();
        while (nextException != null) {
            setNextException(nextException);
            nextException = nextException.getNextException();
        }

        for (Throwable suppressed : exception.getSuppressed()) {
            addSuppressed(suppressed);
        }
    }

    public DatabendSQLException(Throwable cause) {
        super(cause);
    }

    private static String formatMessage(String message, String queryId) {
        if (queryId == null || queryId.isEmpty()) {
            return message;
        }
        if (message == null || message.isEmpty()) {
            return QUERY_ID_PREFIX + queryId;
        }
        if (message.contains(QUERY_ID_PREFIX + queryId)) {
            return message;
        }
        return message + " [" + QUERY_ID_PREFIX + queryId + "]";
    }
}
