package com.databend.jdbc;

import java.sql.SQLException;

final class SqlExceptions {
    private SqlExceptions() {
    }

    static SQLException findSQLException(Throwable throwable) {
        Throwable current = throwable;
        while (current != null) {
            if (current instanceof SQLException) {
                return (SQLException) current;
            }
            current = current.getCause();
        }
        return null;
    }
}
