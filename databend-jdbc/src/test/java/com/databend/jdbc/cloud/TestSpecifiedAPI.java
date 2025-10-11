package com.databend.jdbc.cloud;

import com.databend.client.data.DatabendDataType;
import com.databend.client.data.DatabendRawType;
import com.databend.jdbc.*;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.*;


public class TestSpecifiedAPI {
    static String port = System.getenv("DATABEND_TEST_CONN_PORT") != null ? System.getenv("DATABEND_TEST_CONN_PORT").trim() : "8000";
    static String username = "databend";
    static String password = "databend";

    public static String baseURL() {
        return "jdbc:databend://localhost:" + port;
    }

    public static Connection createConnection()
            throws SQLException {
        return DriverManager.getConnection(baseURL(), username, password);
    }

    @Test(groups = {"IT"})
    public void testUnwrap()
            throws SQLException {
        try (Connection conn = createConnection()) {
            DatabendConnection connection = conn.unwrap(DatabendConnection.class);


            // DatabendStatement: no specified public method
            DatabendStatement statement = connection.createStatement().unwrap(DatabendStatement.class);
            statement.execute("create or replace table test_unwrap(c1 int32)");
            //statement.execute("insert into test_unwrap values (1)");
            statement.execute("create or replace stage test_unwrap");

            // DatabendConnection: local file APIs
            String testData = "1234";
            DatabendConnection.LoadMethod m = DatabendConnection.LoadMethod.STAGE;
            InputStream inputStream = new ByteArrayInputStream(testData.getBytes(StandardCharsets.UTF_8));
            connection.uploadStream(inputStream, "test_unwrap", "dir1", "f1", 4, false);

            // DatabendPreparedStatement: no specified public method
            DatabendPreparedStatement ps = connection.prepareStatement("select 1").unwrap(DatabendPreparedStatement.class);

            try(DatabendResultSet rs =  statement.executeQuery("select * from test_unwrap").unwrap(DatabendResultSet.class)) {
                Assert.assertEquals(rs.columnIndex("c1"), 1);
            }

            DatabendDatabaseMetaData dbMeta = connection.getMetaData().unwrap(DatabendDatabaseMetaData.class);
            try (ResultSet rs = dbMeta.getColumns(null, "default", "test_unwrap", new String[]{"c1"})) {
                Assert.assertTrue(rs.next());
                Assert.assertEquals(rs.getString("COLUMN_NAME"), "c1");
                Assert.assertFalse(rs.next());
            }
        }
    }
    @Test(groups = {"UNIT"})
    public void testUtils() {
        ConnectionProperties.allProperties();
        ConnectionProperties.getDefaults();

        DatabendRawType rawType = new DatabendRawType("Nullable(int32)");
        DatabendDataType t = rawType.getDataType();
        Assert.assertEquals(t, DatabendDataType.INT_32);
        DatabendColumnInfo.Builder builder = DatabendColumnInfo.newBuilder("c1", rawType);
        DatabendColumnInfo ci = builder.build();
        Assert.assertEquals(ci.getColumnName(), "c1");
        Assert.assertEquals(new JdbcTypeMapping().toSqlType(ci), Types.INTEGER);
    }
}
