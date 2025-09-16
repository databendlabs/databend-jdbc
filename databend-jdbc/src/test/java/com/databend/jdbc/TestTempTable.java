package com.databend.jdbc;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class TestTempTable {

    void checkNoTempTable() throws SQLException {
        try (Connection c1 = Utils.createConnection();
             Statement statement = c1.createStatement()) {
            for (int i = 0; i < 3; i++) {
                ResultSet rs = statement.executeQuery("SELECT name FROM system.temporary_tables");
                Assert.assertFalse(rs.next());
            }
        }
    }

    @Test(groups = {"IT"})
    public void testTempTable() throws SQLException {
        try (Connection c1 = Utils.createConnection()) {

            // test drop table
            try (Statement statement = c1.createStatement()) {
                for (int i = 0; i < 10; i++) {
                    String tableName = "test_temp_table_" + i;
                    statement.execute(String.format("create or replace temp table %s(i int)", tableName));
                    statement.execute(String.format("insert into %s values (1), (2)", tableName));
                    statement.executeQuery("select * from " + tableName);
                    ResultSet rs = statement.getResultSet();
                    Assert.assertTrue(rs.next());
                    Assert.assertEquals(1, rs.getInt(1));
                    Assert.assertTrue(rs.next());
                    Assert.assertEquals(2, rs.getInt(1));
                    Assert.assertFalse(rs.next());
                    statement.execute("drop table " + tableName);
                }
                ResultSet rs = statement.executeQuery("SELECT name FROM system.temporary_tables where is_current_session = true");
                Assert.assertFalse(rs.next());
                checkNoTempTable();

                // closed when close connection
                statement.execute("create or replace temp table test_temp_table(i int)");
            }
        }
        checkNoTempTable();
    }
}
