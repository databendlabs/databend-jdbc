package com.databend.jdbc.examples;

import jdk.internal.org.jline.utils.Log;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

class Examples {
    private static Connection createConnection()
            throws SQLException {
        String url = "jdbc:databend://localhost:8000";
        return DriverManager.getConnection(url, "databend", "databend");
    }

    public static void main(String[] args) throws SQLException {
        // set up
        Connection c = createConnection();
        Log.info("-----------------");
        Log.info("Databend JDBC Examples");
        // execute demo
        c.createStatement().execute("drop table if exists test_prepare_statement");
        c.createStatement().execute("create table test_prepare_statement (a int, b string)");


        // insert into with PreparedStatement
        String sql = "insert into test_prepare_statement values (?,?)";
        Connection conn = createConnection();
        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setInt(1, 1);
            statement.setString(2, "b");
            statement.addBatch();
            int[] result = statement.executeBatch();
            Log.info("Insert result: " + result[0]);
        }
        // update with PreparedStatement
        String updateSQL = "update test_prepare_statement set b = ? where a = ?";
        try (PreparedStatement statement = conn.prepareStatement(updateSQL)) {
            statement.setInt(2, 1);
            statement.setString(1, "c");
            int result = statement.executeUpdate();
            Log.info("Update result: " + result);
        }

        // executeQuery and return ResultSet
        ResultSet r = conn.createStatement().executeQuery("select * from test_prepare_statement");
        while (r.next()) {
            Log.info("Row: " + r.getInt(1) + ", " + r.getString(2));
        }

        // replace into with PreparedStatement
        String replaceIntoSQL = "replace into test_prepare_statement on(a) values (?,?)";
        try (PreparedStatement statement = conn.prepareStatement(replaceIntoSQL)) {
            statement.setInt(1, 1);
            statement.setString(2, "d");
            statement.addBatch();
            int[] result = statement.executeBatch();
            Log.info("Replace into result: " + result[0]);
        }
        ResultSet r2 = conn.createStatement().executeQuery("select * from test_prepare_statement");
        while (r2.next()) {
            Log.info("Row: " + r2.getInt(1) + ", " + r2.getString(2));
        }
        // delete with PreparedStatement
        String deleteSQL = "delete from test_prepare_statement where a = ?";
        try (PreparedStatement statement = conn.prepareStatement(deleteSQL)) {
            statement.setInt(1, 1);
            int result = statement.executeUpdate();
            Log.info("Delete result: " + result);
        }
        ResultSet r3 = conn.createStatement().executeQuery("select * from test_prepare_statement");
//        Assert.assertEquals(0, r3.getRow());
        while (r3.next()) {
            Log.info("Row: " + r3.getInt(1) + ", " + r3.getString(2));
        }
    }
}

