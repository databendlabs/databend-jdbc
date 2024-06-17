package com.databend.jdbc.exception;

import java.sql.SQLException;

public class DatabendWithQueryIdSqlException extends SQLException {
    public DatabendWithQueryIdSqlException() {
        super();
    }

    public DatabendWithQueryIdSqlException(String message, String queryId) {
        super(message, queryId);
    }

    public DatabendWithQueryIdSqlException(String message, String queryId, Throwable cause) {
        super(message, queryId, cause);
    }

    public DatabendWithQueryIdSqlException(Throwable cause) {
        super(cause);
    }
}
