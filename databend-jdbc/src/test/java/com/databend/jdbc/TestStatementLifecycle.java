package com.databend.jdbc;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

@Test(timeOut = 10000)
public class TestStatementLifecycle {
    @Test(groups = {"IT"})
    public void testClosingResultSetLeavesStatementReusable() throws SQLException {
        try (Connection connection = Utils.createConnection();
             Statement statement = connection.createStatement()) {
            ResultSet first = statement.executeQuery("select number from numbers(3) order by number");
            Assert.assertTrue(first.next());
            Assert.assertEquals(first.getInt(1), 0);

            first.close();
            Assert.assertTrue(first.isClosed());
            Assert.assertFalse(statement.isClosed());

            try (ResultSet second = statement.executeQuery("select 1")) {
                Assert.assertTrue(second.next());
                Assert.assertEquals(second.getInt(1), 1);
                Assert.assertFalse(second.next());
            }
        }
    }

    @Test(groups = {"IT"})
    public void testClosingStatementClosesCurrentResultSet() throws SQLException {
        try (Connection connection = Utils.createConnection()) {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("select number from numbers(3) order by number");
            Assert.assertTrue(resultSet.next());

            statement.close();

            Assert.assertTrue(statement.isClosed());
            Assert.assertTrue(resultSet.isClosed());
            SQLException exception = Assert.expectThrows(SQLException.class, resultSet::next);
            Assert.assertTrue(exception.getMessage().toLowerCase().contains("closed"));
        }
    }

    @Test(groups = {"IT"})
    public void testGetMoreResultsClosesCurrentResultSet() throws SQLException {
        try (Connection connection = Utils.createConnection();
             Statement statement = connection.createStatement()) {
            ResultSet resultSet = statement.executeQuery("select 1");
            Assert.assertTrue(resultSet.next());
            Assert.assertFalse(statement.getMoreResults(Statement.CLOSE_CURRENT_RESULT));
            Assert.assertTrue(resultSet.isClosed());
            Assert.assertNull(statement.getResultSet());
        }
    }

    @Test(groups = {"IT"})
    public void testConnectionCloseClosesOpenStatementsAndResultSets() throws SQLException {
        Connection connection = Utils.createConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("select number from numbers(3) order by number");
        Assert.assertTrue(resultSet.next());

        connection.close();

        Assert.assertTrue(connection.isClosed());
        Assert.assertTrue(statement.isClosed());
        Assert.assertTrue(resultSet.isClosed());
        SQLException statementException = Assert.expectThrows(SQLException.class, () -> statement.executeQuery("select 1"));
        Assert.assertTrue(statementException.getMessage().toLowerCase().contains("closed"));
    }
}
