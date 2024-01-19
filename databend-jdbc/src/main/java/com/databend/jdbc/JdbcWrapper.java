package com.databend.jdbc;

import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;

public abstract class JdbcWrapper {
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isAssignableFrom(getClass())) {
            return iface.cast(this);
        }

        throw unsupportedError("Cannot unwrap to " + iface.getName());
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isAssignableFrom(getClass());
    }

    public static SQLFeatureNotSupportedException unsupportedError(String message) {
        return new SQLFeatureNotSupportedException(message);
    }
}
