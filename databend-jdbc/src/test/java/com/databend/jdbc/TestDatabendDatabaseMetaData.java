package com.databend.jdbc;

import org.mockito.internal.matchers.Null;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestDatabendDatabaseMetaData {
    private static void assertTableMetadata(ResultSet rs)
            throws SQLException {
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

    @BeforeTest
    public void setUp()
            throws SQLException {
        // create table
        Connection c = createConnection();
        c.createStatement().execute("drop table if exists test_column_meta");
        c.createStatement().execute("drop table if exists decimal_test");
        c.createStatement().execute("create table test_column_meta (nu1 uint8 null, u1 uint8, u2 uint16, u3 uint32, u4 uint64, i1 int8, i2 int16, i3 int32, i4 int64, f1 float32, f2 float64, s1 string,d1 date, d2 datetime, v1 variant, a1 array(int64), t1 Tuple(x Int64, y Int64 NULL)) engine = fuse");
        c.createStatement().execute("create table decimal_test (a decimal(4,2))");
        // json data
    }

    private Connection createConnection()
            throws SQLException {
        String url = "jdbc:databend://localhost:8000";
        return DriverManager.getConnection(url, "databend", "databend");
    }

    @Test(groups = {"IT"})
    public void testGetUrl() throws SQLException {
        try (Connection c = createConnection()) {
            DatabaseMetaData metaData = c.getMetaData();
            String url = metaData.getURL();
            Assert.assertEquals(url, "jdbc:databend://http://localhost:8000");
        }
    }

    @Test(groups = {"IT"})
    public void testGetDatabaseProductVersion()
            throws Exception {
        try (Connection connection = createConnection()) {
            DatabaseMetaData metaData = connection.getMetaData();
            assertEquals(metaData.getDatabaseProductName(), "Databend");
            StringBuilder sb = new StringBuilder();
            sb.append(metaData.getDatabaseMinorVersion());
            Assert.assertTrue(metaData.getDatabaseProductVersion().contains(sb.toString()));
        }
    }

    @Test(groups = {"IT"})
    public void testGetUserName()
            throws Exception {
        try (Connection connection = createConnection()) {
            DatabaseMetaData metaData = connection.getMetaData();
            Assert.assertTrue(metaData.getUserName().contains("databend"));
        }
    }

    @Test(groups = {"IT"})
    public void testGetTables() throws Exception {
        try (Connection connection = createConnection()) {
            DatabaseMetaData metaData = connection.getMetaData();
            try (ResultSet rs = connection.getMetaData().getTables(null, null, null, null)) {
                assertTableMetadata(rs);
            }
        }
    }

    @Test(groups = {"IT"})
    public void testGetSchemas() throws Exception {
        try (Connection connection = createConnection()) {
            DatabaseMetaData metaData = connection.getMetaData();
            try (ResultSet rs = connection.getMetaData().getSchemas()) {
                ResultSetMetaData metaData1 = rs.getMetaData();
                assertEquals(metaData1.getColumnCount(), 2);
            }
            try (ResultSet rs = connection.getMetaData().getCatalogs()) {
                ResultSetMetaData metaData1 = rs.getMetaData();
                assertEquals(metaData1.getColumnCount(), 1);
            }
        }
    }

    @Test(groups = {"IT"})
    public void testGetColumns() throws Exception {
        try (Connection connection = createConnection()) {
            DatabaseMetaData metaData = connection.getMetaData();
            try (ResultSet rs = connection.getMetaData().getColumns(null, null, null, null)) {
                assertEquals(rs.getMetaData().getColumnCount(), 24);
            }
        }
    }

    @Test(groups = {"IT"})
    public void testColumnsMeta() throws Exception {
        try (Connection connection = createConnection()) {
            try (ResultSet rs = connection.getMetaData().getColumns(null, "default", "test_column_meta", null)) {
                while (rs.next()) {
                    String tableSchem = rs.getString("table_schem");
                    String tableName = rs.getString("table_name");
                    String columnName = rs.getString("COLUMN_NAME");
                    int dataType = rs.getInt("data_type");
                    String columnType = rs.getString("type_name");
                    System.out.println(tableSchem + " " + tableName + " " + columnName + " " + dataType + " " + columnType);
                }
            }
            System.out.println("====================================");
            try (ResultSet rs = connection.getMetaData().unwrap(DatabendDatabaseMetaData.class).getColumns(null, "default", "test_column_meta", new String[]{"v1", "u1"})) {
                while (rs.next()) {
                    String tableSchem = rs.getString("table_schem");
                    String tableName = rs.getString("table_name");
                    String columnName = rs.getString("COLUMN_NAME");
                    int dataType = rs.getInt("data_type");
                    String columnType = rs.getString("type_name");
                    Object remarks = rs.getObject("remarks");
                    Assert.assertEquals(remarks, null);
                    System.out.println(tableSchem + " " + tableName + " " + columnName + " " + dataType + " " + columnType);
                }
            }
        }
    }

    @Test(groups = {"IT"})
    public void testGetColumnTypesBySelectEmpty() throws Exception {
        try (Connection connection = createConnection()) {
            ResultSet rs = connection.createStatement().executeQuery("select * from test_column_meta where 1=2");
            ResultSetMetaData metaData = rs.getMetaData();
            assertEquals(metaData.getColumnCount(), 17);
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                System.out.println(metaData.getColumnLabel(i) + " " + metaData.getColumnTypeName(i));
            }
        }
    }

    @Test(groups = {"IT"})
    public void testGetColumnTypeWithDecimal() throws Exception {
        try (Connection connection = createConnection()) {
            ResultSet rs = connection.createStatement().executeQuery("select * from decimal_test");
            ResultSetMetaData metaData = rs.getMetaData();

            int decimalSqlType = metaData.getColumnType(1);
            String columnName = metaData.getColumnName(1);
            int precision = metaData.getPrecision(1);
            int scale = metaData.getScale(1);
            assertEquals(decimalSqlType, Types.DECIMAL);
            assertEquals(columnName, "a");
            assertEquals(precision, 4);
            assertEquals(scale, 2);
        }
    }

    @Test(groups = {"IT"})
    public void testGetPrimaryKeys() throws Exception {
        try (Connection connection = createConnection()) {
            DatabaseMetaData metaData = connection.getMetaData();
            try (ResultSet rs = connection.getMetaData().getPrimaryKeys(null, null, null)) {
                assertEquals(rs.getMetaData().getColumnCount(), 6);
            }
        }
    }

    @Test(groups = {"IT"})
    public void testTableTypes() throws Exception {
        try (Connection connection = createConnection()) {
            DatabaseMetaData metaData = connection.getMetaData();
            try (ResultSet rs = metaData.getTableTypes()) {
                assertEquals(rs.getMetaData().getColumnCount(), 1);
                List<String> totalTableTypes = new ArrayList<>();
                while (rs.next()) {
                    totalTableTypes.add(rs.getString(1));
                }
                assertEquals(totalTableTypes.size(), 3);
                assertTrue(totalTableTypes.contains("TABLE"));
                assertTrue(totalTableTypes.contains("VIEW"));
                assertTrue(totalTableTypes.contains("SYSTEM TABLE"));
            }
        }
    }
}
