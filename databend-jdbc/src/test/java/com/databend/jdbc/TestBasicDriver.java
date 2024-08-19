package com.databend.jdbc;

import com.databend.client.DatabendSession;
import com.databend.client.PaginationOptions;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;
import org.junit.Ignore;
import java.sql.*;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.testng.AssertJUnit.assertEquals;

@Test(timeOut = 10000)
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
        c.createStatement().execute("create table test_basic_driver.table_with_null(a int,b varchar default null)");
        c.createStatement().execute("insert into test_basic_driver.table_with_null(a) values(1)");

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
    public void testExecuteInvalidSql() {
        assertThrows(SQLException.class, () -> {
            try (Connection connection = createConnection();
                 Statement statement = connection.createStatement()) {
                statement.execute("create tabl xxx (a int, b varchar)");
            }
        });
    }

    public void testSchema() {
        try (Connection connection = createConnection()) {
            PaginationOptions p = connection.unwrap(DatabendConnection.class).getPaginationOptions();
            Assert.assertEquals(p.getWaitTimeSecs(), PaginationOptions.getDefaultWaitTimeSec());
            Assert.assertEquals(p.getMaxRowsInBuffer(), PaginationOptions.getDefaultMaxRowsInBuffer());
            Assert.assertEquals(p.getMaxRowsPerPage(), PaginationOptions.getDefaultMaxRowsPerPage());
            DatabendStatement statement = (DatabendStatement) connection.createStatement();
            statement.execute("set global timezone='Asia/Shanghai';");
            statement.execute("SELEcT schema_name as TABLE_SCHEM, catalog_name as TABLE_CATALOG FROM information_schema.schemata where schema_name = 'default' order by catalog_name, schema_name");
            ResultSet r = statement.getResultSet();

            while (r.next()) {
                System.out.println(r.getString(1));
            }
            connection.close();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    @Test
    public void testCreateUserFunction() throws SQLException {
        String s = "create or replace function add_plus(int,int)\n" +
                "returns int\n" +
                "language javascript\n" +
                "handler = 'add_plus_js'\n" +
                "as\n" +
                "$$\n" +
                "export function add_plus_js(i,k){\n" +
                "    return i+k;\n" +
                "}\n" +
                "$$;";
        try (Connection connection = createConnection()) {
            DatabendStatement statement = (DatabendStatement) connection.createStatement();
            statement.execute(s);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        try (Connection connection = createConnection()) {
            DatabendStatement statement = (DatabendStatement) connection.createStatement();
            statement.execute("select add_plus(1,2)");
            ResultSet r = statement.getResultSet();
            r.next();
            Assert.assertEquals(r.getInt(1), 3);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    @Test
    public void TestMergeinto() throws SQLException {
        try (Connection connection = createConnection()) {
            DatabendStatement statement = (DatabendStatement) connection.createStatement();
            statement.execute("CREATE TABLE IF NOT EXISTS test_basic_driver.target_table (\n" +
                    "    ID INT,\n" +
                    "    Name VARCHAR(50),\n" +
                    "    Age INT,\n" +
                    "    City VARCHAR(50)\n" +
                    ");");
            statement.execute("INSERT INTO test_basic_driver.target_table (ID, Name, Age, City)\n" +
                    "VALUES\n" +
                    "    (1, 'Alice', 25, 'Toronto'),\n" +
                    "    (2, 'Bob', 30, 'Vancouver'),\n" +
                    "    (3, 'Carol', 28, 'Montreal');");
            statement.execute("CREATE TABLE IF NOT EXISTS test_basic_driver.source_table (\n" +
                    "    ID INT,\n" +
                    "    Name VARCHAR(50),\n" +
                    "    Age INT,\n" +
                    "    City VARCHAR(50)\n" +
                    ");");
            statement.execute("INSERT INTO test_basic_driver.source_table (ID, Name, Age, City)\n" +
                    "VALUES\n" +
                    "    (1, 'David', 27, 'Calgary'),\n" +
                    "    (2, 'Emma', 29, 'Ottawa'),\n" +
                    "    (4, 'Frank', 32, 'Edmonton');");
            statement.execute("set enable_experimental_merge_into = 1");
            statement.execute("MERGE INTO test_basic_driver.target_table AS T\n" +
                    "    USING (SELECT * FROM test_basic_driver.source_table) AS S\n" +
                    "    ON T.ID = S.ID\n" +
                    "    WHEN MATCHED THEN\n" +
                    "        UPDATE *\n" +
                    "    WHEN NOT MATCHED THEN\n" +
                    "    INSERT *;\n");
            ResultSet r = statement.getResultSet();
            r.next();
            Assert.assertEquals(6, statement.getUpdateCount());
            System.out.println(statement.getUpdateCount());
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    @Test
    public void testWriteDouble() throws SQLException {
        try (Connection connection = createConnection()) {
            DatabendStatement statement = (DatabendStatement) connection.createStatement();
            statement.execute("CREATE TABLE IF NOT EXISTS test_basic_driver.table_double (\n" +
                    "    ID INT,\n" +
                    "    Name VARCHAR(50),\n" +
                    "    Age INT,\n" +
                    "    City VARCHAR(50),\n" +
                    "    Score DOUBLE\n" +
                    ");");
            Double infDouble = Double.POSITIVE_INFINITY;

            String sql = "INSERT INTO test_basic_driver.table_double (ID, Name, Age, City, Score) values";
            PreparedStatement prepareStatement = connection.prepareStatement(sql);
            prepareStatement.setInt(1, 1);
            prepareStatement.setString(2, "Alice");
            prepareStatement.setInt(3, 25);
            prepareStatement.setString(4, "Toronto");
            prepareStatement.setDouble(5, infDouble);

            prepareStatement.addBatch();
            prepareStatement.executeBatch();
            statement.execute("SELECT * FROM test_basic_driver.table_double");
            ResultSet r = statement.getResultSet();
            r.next();
            Assert.assertEquals(r.getDouble(5), Double.POSITIVE_INFINITY);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    @Test
    public void testDefaultSelectNullValue() throws SQLException {
        try (Connection connection = createConnection()) {
            DatabendStatement statement = (DatabendStatement) connection.createStatement();
            statement.executeQuery("SELECT a,b from test_basic_driver.table_with_null");
            ResultSet r = statement.getResultSet();
            r.next();
            Assert.assertEquals(r.getInt(1), 1);
            Assert.assertEquals(r.getObject(2), null);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
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

    @Test
    public void testPrepareStatementQuery() throws SQLException {
        String sql = "SELECT number from numbers(100) where number = ? or number = ?";
        Connection conn = createConnection("test_basic_driver");
        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setInt(1, 1);
            statement.setInt(2, 2);
            ResultSet r = statement.executeQuery();
            r.next();
            System.out.println(r.getLong("number"));
            Assert.assertEquals(r.getLong("number"), 1);
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

    @Test(groups = {"IT"})
    public void testSelectWithPreparement()
            throws SQLException {
        try (Connection connection = createConnection()) {
            connection.createStatement().execute("create or replace table test_basic_driver.table_time(t timestamp, d date, ts timestamp)");
            connection.createStatement().execute("insert into test_basic_driver.table_time values('2021-01-01 00:00:00', '2021-01-01', '2021-01-01 00:00:00')");
            PreparedStatement statement = connection.prepareStatement("SELECT * from test_basic_driver.table_time where t < ? and d < ? and ts < ?");
            statement.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
            statement.setDate(2, new Date(System.currentTimeMillis()));
            statement.setTimestamp(3, new Timestamp(System.currentTimeMillis()));
            ResultSet r = statement.executeQuery();
            r.next();
            Assert.assertEquals(r.getString(1), "2021-01-01 00:00:00.000000");
        }
    }

    @Test(groups = {"IT", "FLAKY"})
    public void testSelectGeometry() throws SQLException, ParseException {
        // skip due to failed cluster tests

        try (Connection connection = createConnection()) {
            connection.createStatement().execute("set enable_geo_create_table=1");
            connection.createStatement().execute("CREATE or replace table cities ( id INT, name VARCHAR NOT NULL, location GEOMETRY);");
            connection.createStatement().execute("INSERT INTO cities (id, name, location) VALUES (1, 'New York', 'POINT (-73.935242 40.73061))');");
            connection.createStatement().execute("INSERT INTO cities (id, name, location) VALUES (2, 'Null', null);");
            Statement statement = connection.createStatement();
            try (ResultSet r = statement.executeQuery("select location from cities")) {
                r.next();
                Assert.assertEquals("{\"type\": \"Point\", \"coordinates\": [-73.935242,40.73061]}", r.getObject(1));
                r.next();
                Assert.assertNull(r.getObject(1));
            }

            // set geometry_output_format to wkb
            connection.createStatement().execute("set geometry_output_format='WKB'");
            try (ResultSet r = statement.executeQuery("select location from cities")) {
                r.next();
                byte[] wkb = r.getBytes(1);
                WKBReader wkbReader = new WKBReader();
                Geometry geometry = wkbReader.read(wkb);
                Assert.assertEquals("POINT (-73.935242 40.73061)", geometry.toText());
            }
        }
    }
}
