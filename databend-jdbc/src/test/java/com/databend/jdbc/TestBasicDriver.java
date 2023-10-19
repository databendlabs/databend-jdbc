package com.databend.jdbc;

import com.databend.client.DatabendSession;
import com.databend.client.PaginationOptions;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.sql.*;
import java.util.Properties;

import static org.testng.AssertJUnit.assertEquals;

public class TestBasicDriver {
    private Connection createConnection()
            throws SQLException {
        String url = "jdbc:databend://localhost:8000";
        return DriverManager.getConnection(url, "databend", "databend");
    }

    private Connection createConnection(String database) throws SQLException {
        String url = "jdbc:databend://localhost:8000/" + database;
        return DriverManager.getConnection(url, "databend", "databend");
    }

    private Connection createConnection(String database, Properties p) throws SQLException {
        String url = "jdbc:databend://localhost:8000/" + database;
        return DriverManager.getConnection(url, p);
    }

    @BeforeTest
    public void setUp()
            throws SQLException {
        // create table
        Connection c = createConnection();
        c.createStatement().execute("drop database if exists test_basic_driver");
        c.createStatement().execute("drop database if exists test_basic_driver_2");
        c.createStatement().execute("create database test_basic_driver");
        c.createStatement().execute("create table test_basic_driver.table1(i int)");
        c.createStatement().execute("insert into test_basic_driver.table1 values(1)");
        c.createStatement().execute("create database test_basic_driver_2");

        // json data
    }

    @Test(groups = {"IT"})
    public void testBasic()
            throws SQLException {
        try (Connection connection = createConnection()) {
            PaginationOptions p = connection.unwrap(DatabendConnection.class).getPaginationOptions();
            Assert.assertEquals(p.getWaitTimeSecs(), PaginationOptions.getDefaultWaitTimeSec());
            Assert.assertEquals(p.getMaxRowsInBuffer(), PaginationOptions.getDefaultMaxRowsInBuffer());
            Assert.assertEquals(p.getMaxRowsPerPage(), PaginationOptions.getDefaultMaxRowsPerPage());
            DatabendStatement statement = (DatabendStatement) connection.createStatement();
            statement.execute("SELECT number from numbers(200000) order by number");
            ResultSet r = statement.getResultSet();
            r.next();
            for (int i = 1; i < 1000; i++) {
                r.next();
                Assert.assertEquals(r.getInt(1), i);
            }
            connection.close();
        } finally {

        }
    }

    @Test(groups = {"IT"})
    public void testQueryUpdateCount()
            throws SQLException {
        try (Connection connection = createConnection()) {
            DatabendStatement statement = (DatabendStatement) connection.createStatement();
            statement.execute("SELECT version()");
            ResultSet r = statement.getResultSet();
            r.next();
            assertEquals(-1, statement.getUpdateCount());
        }
    }

    @Test
    public void testPrepareStatementQuery() throws SQLException {
        String sql = "SELECT number from numbers(100) where number = ?";
        Connection connection = createConnection("test_basic_driver");
        try(PreparedStatement statement  = connection.prepareStatement(sql)) {
            statement.setInt(1, 1);
            ResultSet r = statement.executeQuery();
            statement.execute();
            r.next();
            System.out.println(r.getLong("number"));
        }
    }

    @Test(groups = {"IT"})
    public void testBasicWithProperties() throws SQLException {
        Properties p = new Properties();
        p.setProperty("wait_time_secs", "10");
        p.setProperty("max_rows_in_buffer", "100");
        p.setProperty("max_rows_per_page", "100");
        p.setProperty("user", "databend");
        p.setProperty("password", "databend");
        //INFO databend_query::servers::http::v1::http_query_handlers: receive http query: HttpQueryRequest { session_id: None, session: Some(HttpSessionConf { database: Some("test_basic_driver"), keep_server_session_secs: None, settings: None }), sql: "SELECT 1", pagination: PaginationConf { wait_time_secs: 10, max_rows_in_buffer: 100, max_rows_per_page: 100 }, string_fields: true, stage_attachment: None }
        try (Connection connection = createConnection("test_basic_driver", p)) {
            PaginationOptions options = connection.unwrap(DatabendConnection.class).getPaginationOptions();
            Assert.assertEquals(options.getWaitTimeSecs(), 10);
            Assert.assertEquals(options.getMaxRowsInBuffer(), 100);
            Assert.assertEquals(options.getMaxRowsPerPage(), 100);
            Statement statement = connection.createStatement();
            statement.execute("SELECT 1");
            ResultSet r = statement.getResultSet();
            r.next();
            Assert.assertEquals(r.getInt(1), 1);
        }
    }

    @Test(groups = {"IT"})
    public void testBasicWithDatabase()
            throws SQLException {
        try (Connection connection = createConnection("test_basic_driver")) {
            Statement statement = connection.createStatement();
            statement.execute("SELECT i from table1");
            ResultSet r = statement.getResultSet();
            r.next();
            Assert.assertEquals(r.getInt(1), 1);
            r = connection.getMetaData().getColumns(null, null, "table1", null);
            while (r.next()) {
                String tableSchem = r.getString("table_schem");
                String tableName = r.getString("table_name");
                String columnName = r.getString("COLUMN_NAME");
                String dataType = r.getString("data_type");
                String columnType = r.getString("type_name");
                System.out.println(tableSchem + " " + tableName + " " + columnName + " " + dataType + " " + columnType);
            }
            connection.close();
        } finally {

        }
    }

    @Test(groups = {"IT"})
    public void testUpdateSession()
            throws SQLException {
        try (Connection connection = createConnection("test_basic_driver")) {
            connection.createStatement().execute("set max_threads=1");
            connection.createStatement().execute("use test_basic_driver_2");
            DatabendSession session = connection.unwrap(DatabendConnection.class).getSession();
            Assert.assertEquals(session.getDatabase(), "test_basic_driver_2");
            Assert.assertEquals(session.getSettings().get("max_threads"), "1");
        }
    }

    @Test(groups = {"IT"})
    public void testResultException() {
        try (Connection connection = createConnection()) {
            Statement statement = connection.createStatement();
            ResultSet r = statement.executeQuery("SELECT 1e189he 198h");

            connection.close();
        } catch (SQLException e) {
            Assert.assertTrue(e.getMessage().contains("Query failed"));
        }
    }
}
