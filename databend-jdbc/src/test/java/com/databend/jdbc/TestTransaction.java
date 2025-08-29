package com.databend.jdbc;

import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.testng.Assert.*;

public class TestTransaction {

    @BeforeTest
    public void setUp()
            throws SQLException {
        // create db
        try (Connection c = Utils.createConnection()) {
            c.createStatement().execute("create or replace database test_txn");
        }
    }

    @Test(groups = {"IT"})
    public void testCommit() throws SQLException {
        try ( Connection c1 = Utils.createConnection();
              Connection c2 = Utils.createConnection();
              Connection c3 = Utils.createConnection()
        ) {
            c1.createStatement().execute("create or replace table test_txn.table1(i int)");

            try (Statement statement = c1.createStatement()) {
                statement.execute("begin");
                statement.execute("insert into test_txn.table1 values(4)");
                statement.execute("select * from test_txn.table1");
                ResultSet rs = statement.getResultSet();
                assertTrue(rs.next());
                Assert.assertEquals(4, rs.getInt(1));
                assertFalse(rs.next());

            }

            try (Statement statement = c2.createStatement()) {
                statement.execute("begin");
                statement.execute("insert into test_txn.table1 values(5)");
                statement.execute("select * from test_txn.table1");
                ResultSet rs = statement.getResultSet();
                assertTrue(rs.next());
                Assert.assertEquals(5, rs.getInt(1));
                assertFalse(rs.next());
            }
            c1.commit();

            try (Statement statement = c3.createStatement()) {
                statement.execute("select * from test_txn.table1");
                ResultSet rs = statement.getResultSet();
                assertTrue(rs.next());
                Assert.assertEquals(4, rs.getInt(1));
                assertFalse(rs.next());
            }
            c2.commit();

            try (Statement statement = c3.createStatement()) {
                statement.execute("select * from test_txn.table1 order by i");
                ResultSet rs = statement.getResultSet();
                assertTrue(rs.next());
                Assert.assertEquals(4, rs.getInt(1));
                assertTrue(rs.next());
                Assert.assertEquals(5, rs.getInt(1));
                assertFalse(rs.next());
            }
        }
    }

    @Test(groups = {"IT"})
    public void testRollback()
            throws SQLException {
        try (Connection c = Utils.createConnection()) {
            try (Statement statement = c.createStatement()) {
                c.createStatement().execute("create or replace table test_txn.table2(i int)");
                statement.execute("begin");
                statement.execute("select 11");
                statement.execute("insert into test_txn.table2 values(3)");
                statement.execute("rollback");
                statement.execute("select * from test_txn.table2");
                ResultSet rs = statement.getResultSet();
                assertFalse(rs.next());
            }
        }
    }

    @Test(groups = {"IT"})
    public void testConflict() throws SQLException {
        try (Connection c1 = Utils.createConnection(); Connection c2 = Utils.createConnection()) {
            Statement statement1 = c1.createStatement();
            Statement statement2 = c2.createStatement();

            statement1.execute("create or replace table test_txn.table3(i int, j int)");
            statement1.execute("insert into test_txn.table3 values (1, 11)");

            statement1.execute("begin");
            statement1.execute("UPDATE test_txn.table3 set j = 111 where i=1");

            statement2.execute("begin");
            statement2.execute("UPDATE test_txn.table3 set j = 222 where i=1");
            c2.commit();

            java.sql.SQLException exception = Assert.expectThrows(
                    java.sql.SQLException.class,
                    () ->  statement1.execute("commit")
            );
            // e.g. Unresolvable conflict detected for table 2249
            Assert.assertTrue(exception.getMessage().toLowerCase().contains("conflict"), exception.getMessage());


            statement2.execute("select j from test_txn.table3 where i = 1");
            ResultSet rs = statement2.getResultSet();
            assertTrue(rs.next());
            Assert.assertEquals(rs.getInt(1), 222);
            assertFalse(rs.next());
        }

    }
}
