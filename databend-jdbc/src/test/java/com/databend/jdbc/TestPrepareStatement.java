package com.databend.jdbc;

import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class TestPrepareStatement
{
    private Connection createConnection()
            throws SQLException
    {
        String url = "jdbc:databend://localhost:8000";
        return DriverManager.getConnection(url, "root", "root");
    }
    @BeforeTest
    public void setUp()
            throws SQLException
    {
        // create table
        Connection c = createConnection();

        c.createStatement().execute("drop table if exists test_prepare_statement");
        c.createStatement().execute("create table test_prepare_statement (a int, b int)");
    }
    @Test(groups = "IT")
    public void TestBatchInsert() throws SQLException {
        Connection c = createConnection();
        c.setAutoCommit(false);

        PreparedStatement ps = c.prepareStatement("insert into test_prepare_statement values");
        ps.setInt(1, 1);
        ps.setInt(2, 2);
        ps.addBatch();
        ps.setInt(1, 3);
        ps.setInt(2, 4);
        ps.addBatch();
        System.out.println("execute batch insert");
        int[] ans = ps.executeBatch();
        Assert.assertEquals(ans.length, 2);
        Assert.assertEquals(ans[0], 1);
        Assert.assertEquals(ans[1], 1);
        Statement statement = c.createStatement();

        System.out.println("execute select");
        statement.execute("SELECT * from test_prepare_statement");
        ResultSet r = statement.getResultSet();

        while (r.next()) {
            System.out.println(r.getInt(1));
            System.out.println(r.getInt(2));
        }
//        r.next();
//        Assert.assertEquals(r.getInt(1), 1);
//        Assert.assertEquals(r.getInt(2), 2);
//        r.next();
//        Assert.assertEquals(r.getInt(1), 3);
//        Assert.assertEquals(r.getInt(2), 4);

    }
}
