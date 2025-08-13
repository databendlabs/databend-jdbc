package com.databend.jdbc;

import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class TestTransaction {

    @BeforeTest
    public void setUp()
            throws SQLException {
        // create table
        Connection c = Utils.createConnection();
        c.createStatement().execute("drop database if exists test_txn");
        c.createStatement().execute("create database test_txn");
        c.createStatement().execute("create table test_txn.table1(i int)");
    }

    @Test
    public void testRollback()
            throws SQLException {
        Connection c = Utils.createConnection();
        c.createStatement().execute("delete from test_txn.table1");
        try (Statement statement = c.createStatement()) {
            statement.execute("begin");
            ResultSet r = statement.getResultSet();
        }

        try (Statement statement = c.createStatement()) {
            statement.execute("select 11");
            // txn_state = Auto_Commit, not correct, should be Active
            ResultSet r = statement.getResultSet();
            while (r.next()) {
            }
        }

        try (Statement statement = c.createStatement()) {
            statement.execute("insert into test_txn.table1 values(3)");
            ResultSet r = statement.getResultSet();
        }
        try (Statement statement = c.createStatement()) {
            statement.execute("rollback");
            ResultSet r = statement.getResultSet();
        }

        try (Statement statement = c.createStatement()) {
            statement.execute("select * from test_txn.table1");
            ResultSet rs = statement.getResultSet();
            while (rs.next()) {
                Assert.assertEquals(0, rs.getInt(1));
            }
        }
    }

    @Test
    public void testCommit() throws SQLException {
        Connection c1 = Utils.createConnection();
        Connection c2 = Utils.createConnection();
        c1.createStatement().execute("delete from test_txn.table1");
        try (Statement statement = c1.createStatement()) {
            statement.execute("create  or replace table test_txn.table1(i int)");
            statement.execute("begin");
            statement.execute("insert into test_txn.table1 values(4)");
            statement.execute("select * from test_txn.table1");
            ResultSet rs = statement.getResultSet();
            while (rs.next()) {
                Assert.assertEquals(4, rs.getInt(1));
            }
        }

        try (Statement statement = c2.createStatement()) {
            statement.execute("begin");
            statement.execute("insert into test_txn.table1 values(5)");
            statement.execute("select * from test_txn.table1");
            ResultSet rs = statement.getResultSet();
            while (rs.next()) {
                Assert.assertEquals(5, rs.getInt(1));
            }
        }
        c1.commit();
        Connection c3 = Utils.createConnection();
        try (Statement statement = c3.createStatement()) {
            statement.execute("select * from test_txn.table1");
            ResultSet rs = statement.getResultSet();
            while (rs.next()) {
                Assert.assertEquals(4, rs.getInt(1));
            }
        }
    }

}
