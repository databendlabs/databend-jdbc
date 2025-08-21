package com.databend.jdbc;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class TestTempTable {

    @Test( groups = {"IT"})
    public void testTempTable() throws SQLException {
        try(Connection c1 = Utils.createConnection()) {
            Statement statement= c1.createStatement();
            statement.execute("create or replace temp table table1(i int)");
            statement.execute("insert into table1 values (1), (2)");
            statement.executeQuery("select * from table1");
            ResultSet rs = statement.getResultSet();
            Assert.assertEquals(true, rs.next());
            Assert.assertEquals(1, rs.getInt(1));
            Assert.assertEquals(true, rs.next());
            Assert.assertEquals(2, rs.getInt(1));
            Assert.assertEquals(false, rs.next());
        }
    }
}
