package com.databend.jdbc;

import java.sql.*;
import java.util.Properties;

public class Utils {

    static String port = System.getenv("DATABEND_TEST_CONN_PORT") != null ? System.getenv("DATABEND_TEST_CONN_PORT").trim() : "8000";
    static String username = "databend";
    static String password = "databend";

    public static String baseURL() {
        return "jdbc:databend://localhost:" + port;
    }

    public static String getUsername() {
        return username;
    }

    public static String getPassword() {
        return password;
    }


    public static Connection createConnection()
            throws SQLException {
        return DriverManager.getConnection(baseURL(), username, password);
    }

    public static Connection createConnection(String database) throws SQLException {
        String url = baseURL() + "/" + database;
        return DriverManager.getConnection(url, username, password);
    }

    public static Connection createConnection(String database, Properties p) throws SQLException {
        String url = baseURL() + "/" + database;
        return DriverManager.getConnection(url, p);
    }


    public static Connection createConnectionWithPresignedUrlDisable() throws SQLException {
        String url = baseURL() + "?presigned_url_disabled=true";
        return DriverManager.getConnection(url, "databend", "databend");
    }

    public static int countTable(Statement statement, String table) throws SQLException {
        ResultSet r = statement.executeQuery(String.format("select count(*) from %s", table));
        r.next();
        return r.getInt(1);
    }
}

