package com.databend.jdbc;

import com.databend.client.StageAttachment;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


public class TestPrepareStatement {

    private static final ThreadLocal<String> METHOD_NAME = new ThreadLocal<>();
    private static final ThreadLocal<String> DB_NAME = new ThreadLocal<>();

    @BeforeMethod(groups = "IT")
    public void captureMethod(Method method) {
        METHOD_NAME.set(method.getName());
        String dbName = "TestPrepareStatement_" + METHOD_NAME.get();
        DB_NAME.set(dbName.toLowerCase());
        System.out.println("【DEBUG】DB_NAME = " + METHOD_NAME.get());
    }

    Connection getConn() throws SQLException {
        String dbName = DB_NAME.get();
        Connection c = Utils.createConnection();
        Statement s = c.createStatement();
        s.execute(String.format("create or replace database %s", dbName));
        s.execute(String.format("use %s", dbName));
        return c;
    }

    @Test(groups = "IT")
    public void TestBatchInsert() throws SQLException {
        Statement s;
        int[] c1;
        String[] c2;
        PreparedStatement ps;
        try (Connection c = getConn()) {
            c.setAutoCommit(false);
            s = c.createStatement();
            s.execute("create or replace table t1 (a int, b string)");

            c1 = new int[]{1, 2};
            c2 = new String[]{"a", "b"};

            ps = c.prepareStatement("insert into t1 values");
            for (int i = 0; i < c1.length; i++) {
                ps.setInt(1, c1[i]);
                ps.setString(2, c2[i]);
                ps.addBatch();
            }
            int[] ans = ps.executeBatch();
            Assert.assertEquals(ans, new int[] {1, 1});

            s.execute("SELECT * from t1");
            ResultSet r = s.getResultSet();

            for (int i = 0; i < c1.length; i++) {
                Assert.assertTrue(r.next());
                Assert.assertEquals(r.getInt(1), c1[i]);
                Assert.assertEquals(r.getString(2), c2[i]);
            }
            Assert.assertFalse(r.next());
        }
    }

    @Test(groups = "IT")
    public void TestBatchInsertWithNULL() throws SQLException {
        try (Connection c = getConn();
             Statement s = c.createStatement()) {
            c.setAutoCommit(false);
            s.execute("create or replace table t1 (a int, b string)");
            PreparedStatement ps = c.prepareStatement("insert into t1 values");

            ps.setInt(1, 1);
            ps.setNull(2, Types.NULL);
            ps.addBatch();

            ps.setInt(1, 2);
            ps.setObject(2, null, Types.NULL);
            ps.addBatch();

            int[] ans = ps.executeBatch();
            Assert.assertEquals(ans, new int[] {1, 1});

            Statement statement = c.createStatement();
            statement.execute("SELECT * from t1");
            ResultSet r = statement.getResultSet();

            int[] c1 = {1, 2};
            for (int j : c1) {
                Assert.assertTrue(r.next());
                Assert.assertEquals(r.getInt(1), j);
                Assert.assertNull(r.getString(2));
            }
            Assert.assertFalse(r.next());
        }
    }

    @Test(groups = "UNIT")
    public void TestConvertSQLWithBatchValues() {
        List<String[]> batchValues = new ArrayList<>();
        // Add string arrays to batchValues
        String[] values1 = { "1" };
        String[] values2 = { "2" };
        batchValues.add(values1);
        batchValues.add(values2);

        String originalSql = "delete from table where id = ?";
        String expectedSql = "delete from table where id = 1;\ndelete from table where id = 2;\n";
        Assert.assertEquals(DatabendPreparedStatement.convertSQLWithBatchValues(originalSql, batchValues), expectedSql);

        List<String[]> batchValues1 = new ArrayList<>();
        // Add string arrays to batchValues
        String[] values3 = { "1", "2" };
        String[] values4 = { "3", "4" };
        batchValues1.add(values3);
        batchValues1.add(values4);

        String originalSql1 = "delete from table where id = ? and uuid = ?";
        String expectedSql1 = "delete from table where id = 1 and uuid = 2;\ndelete from table where id = 3 and uuid = 4;\n";
        Assert.assertEquals(DatabendPreparedStatement.convertSQLWithBatchValues(originalSql1, batchValues1),
                expectedSql1);
    }

    @Test(groups = "IT")
    public void TestBatchDelete() throws SQLException {
        try (Connection c = getConn();
             Statement s = c.createStatement()) {
            c.createStatement().execute("create or replace table t1(a int, b string)");

            int[] c1 = {1, 3};
            String[] c2 = {"b", "b"};

            PreparedStatement ps = c.prepareStatement("insert into t1 values");
            for (int i = 0; i < c1.length; i++) {
                ps.setInt(1, c1[i]);
                ps.setString(2, c2[i]);
                ps.addBatch();
            }
            Assert.assertEquals(ps.executeBatch(), new int[] {1, 1});

            s.execute("SELECT * from t1");
            ResultSet r = s.getResultSet();

            for (int i = 0; i < c1.length; i++) {
                Assert.assertTrue(r.next());
                Assert.assertEquals(r.getInt(1), c1[i]);
                Assert.assertEquals(r.getString(2), c2[i]);
            }

            PreparedStatement deletePs = c.prepareStatement("delete from t1 where a = ?");
            deletePs.setInt(1, 1);
            deletePs.addBatch();
            int[] ansDel = deletePs.executeBatch();
            Assert.assertEquals(ansDel.length, 1);
            // todo: fix this, currently == 0
            // Assert.assertEquals(ansDel[0], 1);

            s.execute("SELECT * from t1");
            ResultSet r1 = s.getResultSet();

            int resultCount = 0;
            while (r1.next()) {
                resultCount += 1;
            }
            Assert.assertEquals(resultCount, 1);
        }
    }

    @Test(groups = "IT")
    public void TestBatchInsertWithTime() throws SQLException {
        try (Connection c = getConn();
             Statement s = c.createStatement()) {
            s.execute("create or replace table t1(a DATE, b TIMESTAMP)");
            c.setAutoCommit(false);

            java.sql.Date[] c1 = {Date.valueOf("2020-01-10"), Date.valueOf("1970-01-01"), Date.valueOf("2021-01-01")};
            Timestamp[] c2 = {Timestamp.valueOf("1983-07-12 21:30:55.888"), Timestamp.valueOf("1970-01-01 00:00:01"), Timestamp.valueOf("1970-01-01 00:00:01.234")};

            PreparedStatement ps = c.prepareStatement("insert into t1 values");
            for (int i = 0; i < c1.length; i++) {
                ps.setDate(1, c1[i]);
                ps.setTimestamp(2, c2[i]);
                ps.addBatch();
            }
            Assert.assertEquals(ps.executeBatch(), new int[]{1, 1, 1});

            s.execute("SELECT * from t1");
            ResultSet r = s.getResultSet();

            for (int i = 0; i < c1.length; i++) {
                Assert.assertTrue(r.next());
                Assert.assertEquals(r.getDate(1), c1[i]);
                Assert.assertEquals(r.getTimestamp(2), c2[i]);
            }
            Assert.assertFalse(r.next());
        }
    }

    @DataProvider(name = "complexDataType")
    private Object[][] provideTestData() {
        return new Object[][] {
                {true, false},
                {true, true},
                {false, false},
        };
    }

    @Test(groups = "IT", dataProvider = "complexDataType")
    public void TestBatchInsertWithComplexDataType(boolean presigned,  boolean placeholder) throws SQLException {
        String tableName = String.format("test_object_%s_%s", presigned, placeholder).toLowerCase();
        try (Connection c = presigned ? Utils.createConnection() : Utils.createConnectionWithPresignedUrlDisable();
             Statement s = c.createStatement()
        ) {
            c.setAutoCommit(false);
            s.execute("create or replace database test_prepare_statement");
            s.execute("use test_prepare_statement");
            String createTableSQL = String.format(
                    "CREATE OR replace table %s(id TINYINT, obj VARIANT, d TIMESTAMP, s String, arr ARRAY(INT64)) Engine = Fuse"
                    , tableName);
            s.execute(createTableSQL);
            String insertSQL = String.format("insert into %s values %s", tableName, placeholder ? "(?,?,?,?,?)" : "");

            PreparedStatement ps = c.prepareStatement(insertSQL);
            ps.setInt(1, 1);
            ps.setString(2, "{\"a\": 1,\"b\": 2}");
            ps.setTimestamp(3, Timestamp.valueOf("1983-07-12 21:30:55.888"));
            ps.setString(4, "hello world, 你好");
            ps.setString(5, "[1,2,3,4,5]");
            ps.addBatch();
            int[] ans = ps.executeBatch();
            Assert.assertEquals(ans.length, 1);
            Assert.assertEquals(ans[0], 1);

            s.execute(String.format("SELECT * from %s", tableName));
            ResultSet r = s.getResultSet();

            Assert.assertTrue(r.next());
            Assert.assertEquals(r.getInt(1), 1);
            Assert.assertEquals(r.getString(2), "{\"a\":1,\"b\":2}");
            Assert.assertEquals(Timestamp.valueOf(r.getString(3)), Timestamp.valueOf("1983-07-12 21:30:55.888"));
            Assert.assertEquals(r.getString(4), "hello world, 你好");
            Assert.assertEquals(r.getString(5), "[1,2,3,4,5]");

            Assert.assertFalse(r.next());
        }
    }

    @Test(groups = "IT")
    public void TestBatchReplaceInto() throws SQLException {
        try (Connection c = getConn();
             Statement s = c.createStatement()) {
            s.execute("create or replace table t1(a int, b string)");
            PreparedStatement ps1 = c.prepareStatement("insert into t1 values");
            ps1.setInt(1, 1);
            ps1.setInt(2, 2);
            ps1.addBatch();
            ps1.executeBatch();

            PreparedStatement ps = c.prepareStatement("replace into t1 on(a) values");
            int[] c1 = {1, 3};
            String[] c2 = {"b", "b"};

            for (int i = 0; i < c1.length; i++) {
                ps.setInt(1, c1[i]);
                ps.setString(2, c2[i]);
                ps.addBatch();
            }
            Assert.assertEquals(ps.executeBatch(), new int[] {1, 1});

            s.execute("SELECT * from t1");
            ResultSet r = s.getResultSet();

            for (int i = 0; i < c1.length; i++) {
                Assert.assertTrue(r.next());
                Assert.assertEquals(r.getInt(1), c1[i]);
                Assert.assertEquals(r.getString(2), c2[i]);
            }
        }
    }

    @Test(groups = "IT")
    public void testPrepareStatementExecute() throws SQLException {
        try (Connection c = getConn();
            Statement s = c.createStatement()) {
            s.execute("create or replace table t1 (a int, b string)");
            String insertSql = "insert into t1 values (?,?)";
            try (PreparedStatement ps = c.prepareStatement(insertSql)) {
                ps.setInt(1, 1);
                ps.setString(2, "b");
                ps.execute();
            }
            String updateSql = "update t1 set b = ? where a = ?";
            try (PreparedStatement ps = c.prepareStatement(updateSql)) {
                ps.setString(1, "c");
                ps.setInt(2, 1);
                ps.execute();
            }

            String selectSql = "select * from t1";
            try (ResultSet rs = s.executeQuery(selectSql)) {
                while (rs.next()) {
                    Assert.assertEquals("c", rs.getString(2));
                }
            }

            String deleteSql = "delete from t1 where a = ?";
            try (PreparedStatement ps = c.prepareStatement(deleteSql)) {
                ps.setInt(1, 1);
                ps.execute();
            }

            try (ResultSet rs = s.executeQuery(selectSql)) {
                Assert.assertEquals(0, rs.getRow());
                Assert.assertFalse(rs.next());
            }
        }
    }

    @Test(groups = "IT")
    public void testUpdateSetNull() throws SQLException {
        try (Connection conn = getConn();
             Statement s = conn.createStatement()) {
            s.execute("create or replace table t1(a int, b string)");
            String sql = "insert into t1 values (?,?)";
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setInt(1, 1);
                statement.setString(2, "b");
                statement.addBatch();
                int[] result = statement.executeBatch();
                System.out.println(result);
                Assert.assertEquals(1, result.length);
            }
            String updateSQL = "update t1 set b = ? where a = ?";
            try (PreparedStatement statement = conn.prepareStatement(updateSQL)) {
                statement.setInt(2, 1);
                statement.setNull(1, Types.NULL);
                int result = statement.executeUpdate();
                System.out.println(result);
                Assert.assertEquals(1, result);
            }
            try (PreparedStatement statement = conn
                    .prepareStatement("select a, regexp_replace(b, '\\d', '*') from t1 where a = ?")) {
                statement.setInt(1, 1);
                ResultSet r = statement.executeQuery();
                while (r.next()) {
                    Assert.assertEquals(1, r.getInt(1));
                    Assert.assertNull(r.getString(2));
                }
            }
            String insertSelectSql = "insert overwrite t1 select * from t1";
            try (PreparedStatement ps = conn.prepareStatement(insertSelectSql)) {
                ps.execute();
            }
        }
    }

    @Test(groups = "IT")
    public void testUpdateStatement() throws SQLException {
        try (Connection conn = getConn();
             Statement s = conn.createStatement()) {
            s.execute("create or replace table t1(a int, b string)");
            String sql = "insert into t1 values (?,?)";
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setInt(1, 1);
                statement.setString(2, "b");
                statement.addBatch();
                int[] result = statement.executeBatch();
                System.out.println(result);
                Assert.assertEquals(1, result.length);
            }
            String updateSQL = "update t1 set b = ? where a = ?";
            try (PreparedStatement statement = conn.prepareStatement(updateSQL)) {
                statement.setInt(2, 1);
                statement.setObject(1, "c'c");
                int result = statement.executeUpdate();
                System.out.println(result);
                Assert.assertEquals(1, result);
            }
            try (PreparedStatement statement = conn
                    .prepareStatement("select a, regexp_replace(b, '\\d', '*') from t1 where a = ?")) {
                statement.setInt(1, 1);
                ResultSet r = statement.executeQuery();
                while (r.next()) {
                    Assert.assertEquals(1, r.getInt(1));
                    Assert.assertEquals("c'c", r.getString(2));
                }
            }
        }
    }

    @Test(groups = "IT")
    public void testAllPreparedStatement() throws SQLException {
        try (Connection conn = getConn();
             Statement s = conn.createStatement()) {
            s.execute("create or replace table t1(a int, b string)");
            String sql = "insert into t1 values (?,?)";
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setInt(1, 1);
                statement.setString(2, "b");
                statement.addBatch();
                statement.setInt(1, 2);
                statement.setString(2, "z");
                statement.addBatch();
                statement.setInt(1, 3);
                statement.setString(2, "x");
                statement.addBatch();
                statement.setInt(1, 4);
                statement.setString(2, "dd");
                statement.addBatch();
                statement.setInt(1, 5);
                statement.setString(2, "ddd");
                statement.addBatch();
                int[] result = statement.executeBatch();
                System.out.println(result);
                Assert.assertEquals(5, result.length);
            }
            String updateSQL = "update t1 set b = ? where b = ?";
            try (PreparedStatement statement = conn.prepareStatement(updateSQL)) {
                statement.setString(1, "c");
                statement.setString(2, "b");
                int result = statement.executeUpdate();
                Assert.assertEquals(1, result);
            }
            try (PreparedStatement statement = conn
                    .prepareStatement("select a, regexp_replace(b, '\\d', '*') from t1 where b = ?")) {
                statement.setString(1, "c");
                ResultSet r = statement.executeQuery();
                while (r.next()) {
                    Assert.assertEquals(1, r.getInt(1));
                    Assert.assertEquals("c", r.getString(2));
                }
            }
            String replaceIntoSQL = "replace into t1 on(a) values (?,?)";
            try (PreparedStatement statement = conn.prepareStatement(replaceIntoSQL)) {
                statement.setInt(1, 1);
                statement.setString(2, "d");
                statement.addBatch();
                Assert.assertEquals(statement.executeBatch(), new int[]{1});

            }
            ResultSet r2 = conn.createStatement().executeQuery("select * from t1");
            while (r2.next()) {
                System.out.println(r2.getInt(1));
                System.out.println(r2.getString(2));
            }

            String deleteSQL = "delete from t1 where a = ?";
            try (PreparedStatement statement = conn.prepareStatement(deleteSQL)) {
                statement.setInt(1, 1);
                boolean result = statement.execute();
                System.out.println(result);
            }

            String deleteSQLVarchar = "delete from t1 where b = ?";
            try (PreparedStatement statement = conn.prepareStatement(deleteSQLVarchar)) {
                statement.setString(1, "1");
                int result = statement.executeUpdate();
                System.out.println(result);
            }

            ResultSet r3 = conn.createStatement().executeQuery("select * from t1");
            Assert.assertEquals(0, r3.getRow());
            while (r3.next()) {
                // noting print
                System.out.println(r3.getInt(1));
                System.out.println(r3.getString(2));
            }
        }
    }

    @Test(groups = "IT")
    public void shouldBuildStageAttachmentWithFileFormatOptions() throws SQLException {
        Connection conn = Utils.createConnection();
        Assert.assertEquals("", conn.unwrap(DatabendConnection.class).binaryFormat());
        StageAttachment stageAttachment = DatabendPreparedStatement.buildStateAttachment((DatabendConnection) conn,
                "stagePath");

        Assert.assertFalse(stageAttachment.getFileFormatOptions().containsKey("binary_format"));
        Assert.assertTrue(stageAttachment.getFileFormatOptions().containsKey("type"));
        Assert.assertEquals("true", stageAttachment.getCopyOptions().get("PURGE"));
        Assert.assertEquals("\\N", stageAttachment.getCopyOptions().get("NULL_DISPLAY"));
    }

    @Test(groups = "IT")
    public void testSelectWithClusterKey() throws SQLException {
        try (Connection conn = getConn();
             Statement s = conn.createStatement()) {
            s.execute("create or replace table t1(a int, b string)");
            String insertSql = "insert into t1 values (?,?)";
            try (PreparedStatement statement = conn.prepareStatement(insertSql)) {
                statement.setInt(1, 1);
                statement.setString(2, "b");
                statement.addBatch();
                statement.setInt(1, 2);
                statement.setString(2, "c");
                statement.addBatch();
                int[] result = statement.executeBatch();
                System.out.println(result);
                Assert.assertEquals(2, result.length);
            }
            conn.createStatement().execute("alter table t1 cluster by (a)");
            String selectSQL = String.format("select * from clustering_information('%s','t1')", DB_NAME.get());
            try (PreparedStatement statement = conn.prepareStatement(selectSQL)) {
                ResultSet rs = statement.executeQuery();
                int rows = 0;
                while (rs.next()) {
                    Assert.assertEquals("linear", rs.getString(2));
                    rows += 1;
                }
                Assert.assertEquals(1, rows);
            }
        }
    }

    @Test(groups = "IT")
    public void testEncodePass() throws SQLException {
        try (Connection conn = Utils.createConnection();
             Connection conn2 = Utils.createConnection()) {
            conn.createStatement().execute("create user if not exists 'u01' identified by 'mS%aFRZW*GW';");
            conn.createStatement().execute("GRANT ALL PRIVILEGES ON default.* TO 'u01'@'%'");
            Properties p = new Properties();
            p.setProperty("user", "u01");
            p.setProperty("password", "mS%aFRZW*GW");

            conn2.createStatement().execute("select 1");
            conn.createStatement().execute("drop user if exists 'u01'");
        }
    }

    @Test(groups = "IT")
    public void testExecuteUpdate() throws SQLException {
        try (Connection conn = getConn();
             Statement s = conn.createStatement()) {
            s.execute("create or replace table t1(a int, b string)");

            String insertSql = "insert into t1 values (?,?)";
            try (PreparedStatement statement = conn.prepareStatement(insertSql)) {
                statement.setInt(1, 1);
                statement.setString(2, "a");
                statement.executeUpdate();

                statement.setInt(1, 2);
                statement.setString(2, "b");
                statement.executeUpdate();

                statement.setInt(1, 3);
                statement.setString(2, "c");
                statement.executeUpdate();
            }

            String updateSingleSql = "update t1 set b = ? where a = ?";
            try (PreparedStatement statement = conn.prepareStatement(updateSingleSql)) {
                statement.setString(1, "x");
                statement.setInt(2, 1);
                int updatedRows = statement.executeUpdate();
                Assert.assertEquals(1, updatedRows, "only update one row");
            }

            String updateMultiSql = "update t1 set b = ? where a in (?, ?)";
            try (PreparedStatement statement = conn.prepareStatement(updateMultiSql)) {
                statement.setString(1, "y");
                statement.setInt(2, 2);
                statement.setInt(3, 3);
                int updatedRows = statement.executeUpdate();
                Assert.assertEquals(2, updatedRows, "should update two rows");
            }

            String updateAndSql = "update t1 set b = ? where ((a = ?)) and (b = ?)";
            try (PreparedStatement statement = conn.prepareStatement(updateAndSql)) {
                statement.setString(1, "z");
                statement.setInt(2, 2);
                statement.setString(3, "y");
                int updatedRows = statement.executeUpdate();
                Assert.assertEquals(1, updatedRows, "should update one row with and condition");
            }

            String deleteSql = "delete from t1 where a = ?";
            try (PreparedStatement statement = conn.prepareStatement(deleteSql)) {
                statement.setInt(1, 1);
                int deletedRows = statement.executeUpdate();
                Assert.assertEquals(1, deletedRows, "should delete one row");
            }

            ResultSet rs = conn.createStatement().executeQuery("select * from t1 order by a");
            int count = 0;
            while (rs.next()) {
                count++;
                if (count == 1) {
                    Assert.assertEquals(2, rs.getInt(1));
                    Assert.assertEquals("z", rs.getString(2));
                } else if (count == 2) {
                    Assert.assertEquals(3, rs.getInt(1));
                    Assert.assertEquals("y", rs.getString(2));
                }
            }
            Assert.assertEquals(2, count, "should have two rows left in the table");
        }
    }

    @Test(groups = "IT")
    public void testInsertWithSelect() throws SQLException {
        if (Compatibility.skipDriverBugLowerThen("0.3.9")) {
            return;
        }
        try (Connection conn = getConn();
             Statement s = conn.createStatement()) {
            s.execute("create or replace table t1(a int, b string)");

            String insertSql = "insert into t1 select a, b from t1 where b = ?";
            try (PreparedStatement ps = conn.prepareStatement(insertSql)) {
                ps.setString(1, "a");
                int insertedRows = ps.executeUpdate();
                Assert.assertEquals(0, insertedRows, "should not insert any rows as the table is empty");
            }

            // Insert some data
            String insertDataSql = "insert into t1 values (?,?)";
            try (PreparedStatement ps = conn.prepareStatement(insertDataSql)) {
                ps.setInt(1, 1);
                ps.setString(2, "a");
                int insertedRows = ps.executeUpdate();
                // TODO: fix this
                // Assert.assertEquals(1, insertedRows, "should insert 1 rows");
                System.out.println(insertedRows);

                ps.setInt(1, 2);
                ps.setString(2, "b");
                insertedRows = ps.executeUpdate();
                // TODO: fix this
                // Assert.assertEquals(1, insertedRows, "should insert 1 rows");
                System.out.println(insertedRows);
            }

            // Now try to insert again with select
            try (PreparedStatement ps = conn.prepareStatement(insertSql)) {
                ps.setString(1, "a");
                int insertedRows = ps.executeUpdate();
                Assert.assertEquals(1, insertedRows, "should insert 1 row from the select");
            }

            ResultSet rs = conn.createStatement().executeQuery("select * from t1 order by a");
            int count = 0;
            while (rs.next()) {
                count++;
            }
            Assert.assertEquals(3, count, "should have 3 rows in the table after insert with select");
        }
    }
}
