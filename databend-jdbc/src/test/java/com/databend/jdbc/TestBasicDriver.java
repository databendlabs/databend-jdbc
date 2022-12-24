package com.databend.jdbc;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class TestBasicDriver
{
    private Connection createConnection()
            throws SQLException
    {
        String url = "jdbc:databend://localhost:8000";
        return DriverManager.getConnection(url, "root", "root");
    }
    @Test(groups = {"IT"})
    public void testBasic()
            throws SQLException
    {
        try (Connection connection = createConnection()) {
            Statement statement = connection.createStatement();
            statement.execute("SELECT number from numbers(200000) order by number");
            ResultSet r = statement.getResultSet();
            r.next();
            for (int i = 1; i < 1000; i++) {
                r.next();
                Assert.assertEquals(r.getInt(1), i);
            }
            connection.close();
        }
        finally {

        }
    }

    @Test(groups = {"IT"})
    public void testResultException()
    {
        try (Connection connection = createConnection()) {
            Statement statement = connection.createStatement();
            ResultSet r =  statement.executeQuery("SELECT 1e189he 198h");

            connection.close();
        }
        catch (SQLException e) {
            Assert.assertTrue(e.getMessage().contains("Query failed"));
        }
    }
}
