package com.databend.jdbc;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Locale;

import static org.testng.Assert.assertEquals;

public class TestDatabendDatabaseMetaData
{
    private static void assertTableMetadata(ResultSet rs)
            throws SQLException
    {
        ResultSetMetaData metadata = rs.getMetaData();
        assertEquals(metadata.getColumnCount(), 10);

        assertEquals(metadata.getColumnLabel(1).toUpperCase(Locale.US), "TABLE_CAT");
        assertEquals(metadata.getColumnType(1), Types.VARCHAR);

        assertEquals(metadata.getColumnLabel(2).toUpperCase(Locale.US), "TABLE_SCHEM");
        assertEquals(metadata.getColumnType(2), Types.VARCHAR);

        assertEquals(metadata.getColumnLabel(3).toUpperCase(Locale.US), "TABLE_NAME");
        assertEquals(metadata.getColumnType(3), Types.VARCHAR);

        assertEquals(metadata.getColumnLabel(4).toUpperCase(Locale.US), "TABLE_TYPE");
        assertEquals(metadata.getColumnType(4), Types.VARCHAR);

        assertEquals(metadata.getColumnLabel(5).toUpperCase(Locale.US), "REMARKS");
        assertEquals(metadata.getColumnType(5), Types.VARCHAR);

        assertEquals(metadata.getColumnLabel(6).toUpperCase(Locale.US), "TYPE_CAT");
        assertEquals(metadata.getColumnType(6), Types.VARCHAR);

        assertEquals(metadata.getColumnLabel(7).toUpperCase(Locale.US), "TYPE_SCHEM");
        assertEquals(metadata.getColumnType(7), Types.VARCHAR);

        assertEquals(metadata.getColumnLabel(8).toUpperCase(Locale.US), "TYPE_NAME");
        assertEquals(metadata.getColumnType(8), Types.VARCHAR);

        assertEquals(metadata.getColumnLabel(9).toUpperCase(Locale.US), "SELF_REFERENCING_COL_NAME");
        assertEquals(metadata.getColumnType(9), Types.VARCHAR);

        assertEquals(metadata.getColumnLabel(10).toUpperCase(Locale.US), "REF_GENERATION");
        assertEquals(metadata.getColumnType(10), Types.VARCHAR);
    }

    private Connection createConnection()
            throws SQLException
    {
        String url = "jdbc:databend://localhost:8000";
        return DriverManager.getConnection(url, "root", "root");
    }

    @Test(groups = {"IT"})
    public void testGetUrl() throws SQLException{
        try(Connection c = createConnection()) {
            DatabaseMetaData metaData = c.getMetaData();
            String url = metaData.getURL();
            Assert.assertEquals(url, "jdbc:databend://http://localhost:8000");
        }
    }

    @Test(groups = {"IT"})
    public void testGetDatabaseProductVersion()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            DatabaseMetaData metaData = connection.getMetaData();
            assertEquals(metaData.getDatabaseProductName(), "Databend");
            StringBuilder sb = new StringBuilder();
            sb.append(metaData.getDatabaseMajorVersion());
            sb.append(".");
            sb.append(metaData.getDatabaseMinorVersion());
            Assert.assertTrue(metaData.getDatabaseProductVersion().contains(sb.toString()));
        }
    }

    @Test(groups = {"IT"})
    public void testGetUserName()
            throws Exception
    {
        try (Connection connection = createConnection()) {
            DatabaseMetaData metaData = connection.getMetaData();
            Assert.assertTrue(metaData.getUserName().contains("root"));
        }
    }

    @Test(groups = {"IT"})
    public void testGetTables() throws Exception
    {
        try (Connection connection = createConnection()) {
            DatabaseMetaData metaData = connection.getMetaData();
            try(ResultSet rs = connection.getMetaData().getTables(null, null, null, null)) {
                assertTableMetadata(rs);
            }
        }
    }

    @Test(groups = {"IT"})
    public  void testGetSchemas() throws Exception
    {
        try (Connection connection = createConnection()) {
            DatabaseMetaData metaData = connection.getMetaData();
            try(ResultSet rs = connection.getMetaData().getSchemas()) {
                ResultSetMetaData metaData1 = rs.getMetaData();
                assertEquals(metaData1.getColumnCount(), 2);
            }
            try(ResultSet rs = connection.getMetaData().getCatalogs()) {
                ResultSetMetaData metaData1 = rs.getMetaData();
                assertEquals(metaData1.getColumnCount(), 1);
            }
        }
    }

    @Test(groups = {"IT"})
    public void testGetColumns() throws Exception
    {
        try (Connection connection = createConnection()) {
            DatabaseMetaData metaData = connection.getMetaData();
            try(ResultSet rs = connection.getMetaData().getColumns(null, null, null, null)) {
                assertEquals(rs.getMetaData().getColumnCount(), 23);
            }
        }
    }

    @Test(groups = {"IT"})
    public void testGetPrimaryKeys() throws Exception
    {
        try (Connection connection = createConnection()) {
            DatabaseMetaData metaData = connection.getMetaData();
            try(ResultSet rs = connection.getMetaData().getPrimaryKeys(null, null, null)) {
                assertEquals(rs.getMetaData().getColumnCount(), 6);
            }
        }
    }
}
