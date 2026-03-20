package com.databend.jdbc;

import org.testng.annotations.Test;

import java.sql.SQLException;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;

public class TestSqlExceptions {
    @Test
    public void testFindSQLExceptionReturnsNestedSQLException() {
        SQLException sqlException = new SQLException("boom", "abc123");
        RuntimeException nested = new RuntimeException(new RuntimeException(sqlException));

        assertSame(SqlExceptions.findSQLException(nested), sqlException);
    }

    @Test
    public void testFindSQLExceptionReturnsNullWhenMissing() {
        RuntimeException nested = new RuntimeException(new IllegalStateException("boom"));

        assertNull(SqlExceptions.findSQLException(nested));
    }

    @Test
    public void testFindSQLExceptionReturnsDirectSQLException() {
        SQLException sqlException = new SQLException("boom", "abc123");

        assertSame(SqlExceptions.findSQLException(sqlException), sqlException);
        assertEquals(SqlExceptions.findSQLException(sqlException).getSQLState(), "abc123");
    }
}
