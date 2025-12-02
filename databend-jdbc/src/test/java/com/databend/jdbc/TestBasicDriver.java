package com.databend.jdbc;

import com.databend.client.DatabendSession;
import com.databend.client.PaginationOptions;
import com.vdurmont.semver4j.Semver;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;


import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Properties;

import static org.testng.Assert.assertThrows;


@Test(timeOut = 10000)
public class TestBasicDriver {
    @BeforeTest(groups = {"IT"})
    public void setUp()
            throws SQLException {
        // create table
        try (Connection c = Utils.createConnection()) {
            c.createStatement().execute("drop database if exists test_basic_driver");
            c.createStatement().execute("drop database if exists test_basic_driver_2");
            c.createStatement().execute("create database test_basic_driver");
            c.createStatement().execute("create table test_basic_driver.table1(i int)");
            c.createStatement().execute("insert into test_basic_driver.table1 values(1)");
            c.createStatement().execute("create database test_basic_driver_2");
        }
    }

    @Test(groups = {"IT"})
    public void testBasic()
            throws SQLException {
        try (Connection connection = Utils.createConnection()) {
            PaginationOptions p = (PaginationOptions)  Compatibility.invokeMethodNoArg(connection, "getPaginationOptions");
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
        }
    }

//    @Test(groups = {"IT"})
//    public void testRetry()
//            throws SQLException {
//        try (Connection connection = Utils.createConnection();
//             Statement statement = connection.createStatement()) {
//            statement.execute("select * from numbers(1000000)");
//            ResultSet r = statement.getResultSet();
//            for (int b = 1; b < 100; b++) {
//                for (int i = 1; i < 10000; i++) {
//                    if (r.next()) {
//                        r.getInt(1);
//                    } else {
//                        System.out.println("stop");
//                        return;
//                    }
//                }
//                System.out.println("sleep");
//                // restart nginx manually
//                Thread.sleep(1000);
//            }
//        } catch (InterruptedException e) {
//            throw new RuntimeException(e);
//        }
//    }

    @Test(groups = {"IT"})
    public void testExecuteInvalidSql() {
        assertThrows(SQLException.class, () -> {
            try (Connection connection = Utils.createConnection();
                 Statement statement = connection.createStatement()) {
                statement.execute("create tabl xxx (a int, b varchar)");
            }
        });
    }

    @Test(groups = {"IT"})
    public void testSchema() throws SQLException {
        try (Connection connection = Utils.createConnection()) {
            DatabendStatement statement = (DatabendStatement) connection.createStatement();
            statement.execute("set global timezone='Asia/Shanghai';");
            statement.execute("SELEcT schema_name as TABLE_SCHEM, catalog_name as TABLE_CATALOG FROM information_schema.schemata where schema_name = 'default' order by catalog_name, schema_name");
            ResultSet r = statement.getResultSet();

            while (r.next()) {
                System.out.println(r.getString(1));
            }
        }
    }

    @Test(groups = {"IT"})
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
        try (Connection connection = Utils.createConnection();
             Statement statement = connection.createStatement()) {
            statement.execute(s);
        }
        try (Connection connection = Utils.createConnection();
             Statement statement = connection.createStatement()) {
            statement.execute("select add_plus(1,2)");
            ResultSet r = statement.getResultSet();
            r.next();
            Assert.assertEquals(r.getInt(1), 3);
        }
    }

    @Test(groups = {"IT"})
    public void TestMergeInto() throws SQLException {
        try (Connection connection = Utils.createConnection();
             Statement statement = connection.createStatement()
        ) {
            statement.execute("CREATE OR REPLACE TABLE test_basic_driver.target_table (\n" +
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
            statement.execute("CREATE OR REPLACE TABLE test_basic_driver.source_table (\n" +
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
            statement.execute("MERGE INTO test_basic_driver.target_table AS T\n" +
                    "    USING (SELECT * FROM test_basic_driver.source_table) AS S\n" +
                    "    ON T.ID = S.ID\n" +
                    "    WHEN MATCHED THEN\n" +
                    "        UPDATE *\n" +
                    "    WHEN NOT MATCHED THEN\n" +
                    "    INSERT *;\n");
            ResultSet r = statement.getResultSet();

            Assert.assertTrue(r.next());
            Assert.assertEquals(3, statement.getUpdateCount());
        }
    }

    @Test(groups = {"IT"})
    public void testWriteDouble() throws SQLException {
        try (Connection connection = Utils.createConnection()) {
            DatabendStatement statement = (DatabendStatement) connection.createStatement();
            statement.execute("CREATE OR replace TABLE test_basic_driver.table_double (\n" +
                    "    ID INT,\n" +
                    "    Name VARCHAR(50),\n" +
                    "    Age INT,\n" +
                    "    City VARCHAR(50),\n" +
                    "    Score DOUBLE\n" +
                    ");");
            double infDouble = Double.POSITIVE_INFINITY;

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
            Assert.assertTrue(r.next());
            System.out.println(r.getString(2));
            Assert.assertEquals(r.getDouble(5), Double.POSITIVE_INFINITY);
            Assert.assertEquals(r.getInt(1), 1);
        }
    }

    @Test(groups = {"IT"})
    public void testDefaultSelectNullValue() throws SQLException {
        try (Connection connection = Utils.createConnection();
             Statement statement = connection.createStatement()
         ) {
            statement.execute("create or replace table test_basic_driver.table_with_null(a int,b varchar default null, c varchar, d varchar)");
            statement.execute("insert into test_basic_driver.table_with_null(a,b,c,d) values(1,null,'null','NULL')");
            statement.execute("SELECT a,b,c,d from test_basic_driver.table_with_null");
            ResultSet r = statement.getResultSet();
            r.next();
            Assert.assertEquals(r.getInt(1), 1);
            Assert.assertNull(r.getObject(2));
            Assert.assertEquals(r.getObject(3), "null");
            if (Compatibility.skipDriverBugLowerThen("0.3.9")) {
                Assert.assertNull(r.getObject(4));
            } else {
                Assert.assertEquals(r.getObject(4), "NULL");
            }
        }
    }

    @Test(groups = {"IT"})
    public void testQueryUpdateCount()
            throws SQLException {
        try (Connection connection = Utils.createConnection()) {
            DatabendStatement statement = (DatabendStatement) connection.createStatement();
            statement.execute("SELECT version()");
            ResultSet r = statement.getResultSet();
            r.next();
            Assert.assertEquals(-1, statement.getUpdateCount());
        }
    }


    @Test(groups = {"IT"})
    public void testBasicWithProperties() throws SQLException {
        Properties p = new Properties();
        p.setProperty("wait_time_secs", "10");
        p.setProperty("max_rows_in_buffer", "100");
        p.setProperty("max_rows_per_page", "100");
        p.setProperty("user", Utils.getUsername());
        p.setProperty("password", Utils.getPassword());

        //INFO databend_query::servers::http::v1::http_query_handlers: receive http query: HttpQueryRequest { session_id: None, session: Some(HttpSessionConf { database: Some("test_basic_driver"), keep_server_session_secs: None, settings: None }), sql: "SELECT 1", pagination: PaginationConf { wait_time_secs: 10, max_rows_in_buffer: 100, max_rows_per_page: 100 }, string_fields: true, stage_attachment: None }
        try (Connection connection = Utils.createConnection("test_basic_driver", p)) {
            PaginationOptions options = (PaginationOptions)  Compatibility.invokeMethodNoArg(connection, "getPaginationOptions");
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
    public void testPrepareStatementQuery() throws SQLException {
        String sql = "SELECT number from numbers(100) where number = ? or number = ?";
        try (Connection conn = Utils.createConnection("test_basic_driver");
             PreparedStatement statement = conn.prepareStatement(sql)) {
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
        try (Connection connection = Utils.createConnection("test_basic_driver")) {
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
        }
    }

    @Test(groups = {"IT"})
    public void testUpdateSession()
            throws SQLException {
        try (Connection connection = Utils.createConnection("test_basic_driver")) {
            connection.createStatement().execute("set max_threads=1");
            connection.createStatement().execute("use test_basic_driver_2");
            DatabendSession session = (DatabendSession)  Compatibility.invokeMethodNoArg(connection, "getSession");
            Assert.assertEquals(session.getDatabase(), "test_basic_driver_2");
            Assert.assertEquals(session.getSettings().get("max_threads"), "1");
        }
    }

    @Test(groups = {"IT"})
    public void testResultException() {
        assertThrows(SQLException.class, () -> {
            try (Connection connection = Utils.createConnection();
                 Statement statement = connection.createStatement()) {
                statement.execute("SELECT 1e189he 198h");
            }
        });
    }

    @Test(groups = {"IT"})
    public void testSelectWithPreparedStatement()
            throws SQLException {
        try (Connection connection = Utils.createConnection()) {
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
}
