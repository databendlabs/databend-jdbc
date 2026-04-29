package com.databend.jdbc;

import com.databend.jdbc.exception.DatabendSQLException;
import com.databend.jdbc.internal.error.QueryError;
import com.databend.jdbc.internal.query.QueryResults;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.regex.Pattern;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestDatabendSQLException {
    private static final Pattern QUERY_ID_PATTERN = Pattern.compile("query_id=[0-9a-f]{32}");

    @Test
    public void testDatabendWithQueryIdSqlExceptionIncludesQueryIdInMessage() {
        DatabendSQLException exception = new DatabendSQLException("boom", "abc123");

        assertEquals(exception.getMessage(), "boom [query_id=abc123]");
        assertEquals(exception.getSQLState(), "abc123");
    }

    @Test
    public void testResultsExceptionIncludesQueryIdInMessage() {
        QueryResults results = new QueryResults(
                "abc123",
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                QueryError.builder().setCode(1001).setMessage("syntax error").build(),
                null,
                null,
                0L,
                null,
                null,
                null,
                null);

        SQLException exception = AbstractDatabendResultSet.resultsException(results, "select 1");

        assertTrue(exception.getMessage().contains("query_id=abc123"));
        assertEquals(exception.getSQLState(), "1001");
    }

    @Test
    public void testDatabendSQLExceptionPreservesOriginalSQLExceptionInformation() {
        IllegalStateException cause = new IllegalStateException("root cause");
        SQLException original = new SQLException("boom", "42000", 1234, cause);
        SQLException next = new SQLException("next");
        IllegalArgumentException suppressed = new IllegalArgumentException("suppressed");
        original.setNextException(next);
        original.addSuppressed(suppressed);

        DatabendSQLException wrapped = new DatabendSQLException(original, "abc123");

        assertEquals(wrapped.getMessage(), "boom [query_id=abc123]");
        assertEquals(wrapped.getSQLState(), "42000");
        assertEquals(wrapped.getErrorCode(), 1234);
        assertEquals(wrapped.getCause(), cause);
        assertEquals(wrapped.getNextException(), next);
        assertEquals(wrapped.getSuppressed().length, 1);
        assertEquals(wrapped.getSuppressed()[0], suppressed);
    }

    @Test(groups = {"IT"})
    public void testSyntaxErrorFromServerIncludesGeneratedQueryId() throws Exception {
        try (Connection connection = Utils.createConnection();
             Statement statement = connection.createStatement()) {
            SQLException exception = org.testng.Assert.expectThrows(SQLException.class,
                    () -> statement.execute("select * from"));
            assertTrue(exception instanceof DatabendSQLException);
            assertTrue(exception.getMessage().contains("query_id="), exception.getMessage());
            assertTrue(QUERY_ID_PATTERN.matcher(exception.getMessage()).find(), exception.getMessage());
            assertEquals(exception.getSQLState().length(), 32);
        }
    }
}
