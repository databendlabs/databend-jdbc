package com.databend.jdbc;

import com.databend.client.StageAttachment;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeSuite;
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
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.Arrays;
import java.util.Properties;
import java.util.TimeZone;

import static com.databend.jdbc.Utils.countTable;
import static org.testng.Assert.*;


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
    public void TestBatchDelete() throws SQLException {
        if (Compatibility.skipDriverBugLowerThen("0.4.1")) {
            return;
        }
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
            deletePs.setInt(1, 2);
            deletePs.addBatch();
            int[] counts = deletePs.executeBatch();
            Assert.assertEquals(counts, new int[] {1, 0}, Arrays.toString(counts));

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
        try (Connection conn = Utils.createConnectionWithPresignedUrlDisable();
             Statement s = conn.createStatement()) {
            s.execute("create or replace table t1(a int, b string)");
            String sql = "insert into t1 values (?,?)";
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.setInt(1, 1);
                statement.setString(2, "b");
                statement.addBatch();
                int[] result = statement.executeBatch();
                Assert.assertEquals(result, new int[]{1});
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
                Assert.assertEquals(result, new int[]{1});
            }
            String updateSQL = "update t1 set b = ? where a = ?";
            try (PreparedStatement statement = conn.prepareStatement(updateSQL)) {
                statement.setInt(2, 1);
                statement.setObject(1, "c'c");
                int result = statement.executeUpdate();
                Assert.assertEquals(result, 1);
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
                statement.setString(2, "r1");
                statement.addBatch();
                statement.setInt(1, 2);
                statement.setString(2, "r2");
                statement.addBatch();
                statement.setInt(1, 3);
                statement.setString(2, "r3");
                statement.addBatch();
                statement.setInt(1, 4);
                statement.setString(2, "r3");
                statement.addBatch();
                statement.setInt(1, 5);
                statement.setString(2, "r5");
                statement.addBatch();
                int[] result = statement.executeBatch();
                Assert.assertEquals(result, new int[] {1, 1, 1, 1, 1});
            }
            String updateSQL = "update t1 set b = ? where b = ?";
            try (PreparedStatement statement = conn.prepareStatement(updateSQL)) {
                statement.setString(1, "r1_new");
                statement.setString(2, "r1");
                int result = statement.executeUpdate();
                Assert.assertEquals(1, result);
            }
            try (PreparedStatement statement = conn
                    .prepareStatement("select a, b from t1 where b = ?")) {
                statement.setString(1, "r1_new");
                ResultSet r = statement.executeQuery();
                while (r.next()) {
                    Assert.assertEquals(1, r.getInt(1));
                    Assert.assertEquals("r1_new", r.getString(2));
                }
            }
            String replaceIntoSQL = "replace into t1 on(a) values (?,?)";
            try (PreparedStatement statement = conn.prepareStatement(replaceIntoSQL)) {
                statement.setInt(1, 1);
                statement.setString(2, "r1_new2");
                statement.addBatch();
                Assert.assertEquals(statement.executeBatch(), new int[]{1});

            }
            ResultSet r2 = conn.createStatement().executeQuery("select * from t1");
            int n = 0;
            while (r2.next()) {
                n +=1;
            }
            Assert.assertEquals(n, 5);

            String deleteSQL = "delete from t1 where a = ?";
            try (PreparedStatement statement = conn.prepareStatement(deleteSQL)) {
                statement.setInt(1, 1);
                boolean result = statement.execute();
                // TODO: fix this
                // Assert.assertFalse(result);
                System.out.println(result);
                Assert.assertEquals(statement.getUpdateCount(), 1);
            }

            String deleteSQLVarchar = "delete from t1 where b = ?";
            try (PreparedStatement statement = conn.prepareStatement(deleteSQLVarchar)) {
                statement.setString(1, "not exists");
                int result = statement.executeUpdate();
                Assert.assertEquals(result, 0);
            }

            ResultSet r3 = conn.createStatement().executeQuery("select * from t1");
            n = 0;
            while (r3.next()) {
                n +=1;
            }
            Assert.assertEquals(n, 4);
        }
    }

    @Test(groups = "IT")
    public void TestStageFileRemovedAfterBatchInsert() throws SQLException {
        String dbName = ("stage_cleanup_" + UUID.randomUUID()).replace("-", "");
        try (Connection c = Utils.createConnection();
             Statement s = c.createStatement()) {
            c.setAutoCommit(false);
            s.execute("create or replace database " + dbName);
            s.execute("use " + dbName);
            s.execute("create or replace table t_stage_cleanup(a int, b string)");

            Set<String> before = new HashSet<>();
            try (ResultSet rs = s.executeQuery("LIST @~/")) {
                while (rs.next()) {
                    before.add(rs.getString(1));
                }
            }

            try (PreparedStatement ps = c.prepareStatement("insert into t_stage_cleanup values")) {
                ps.setInt(1, 1);
                ps.setString(2, "hello");
                ps.addBatch();
                int[] counts = ps.executeBatch();
                Assert.assertEquals(counts, new int[] {1});
            }

            try (ResultSet rs = s.executeQuery("SELECT a, b FROM t_stage_cleanup")) {
                Assert.assertTrue(rs.next());
                Assert.assertEquals(rs.getInt(1), 1);
                Assert.assertEquals(rs.getString(2), "hello");
                Assert.assertFalse(rs.next());
            }

            Set<String> after = new HashSet<>();
            try (ResultSet rs = s.executeQuery("LIST @~/")) {
                while (rs.next()) {
                    after.add(rs.getString(1));
                }
            }
            Set<String> diff = new HashSet<>(after);
            diff.removeAll(before);
            if (!diff.isEmpty()) {
                Assert.fail("Stage has unexpected leftover entries: " + diff);
            }
        }
    }

    @Test(groups = "UNIT")
    public void shouldBuildStageAttachmentWithFileFormatOptions() throws SQLException {
        Connection conn = Utils.createConnection();
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
                Assert.assertEquals(result, new int[] {1, 1});
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

                // else 0
                if (!Compatibility.skipDriverBugLowerThen("0.4.1")) {
                    Assert.assertEquals(1, insertedRows, "should insert 1 rows");
                }

                ps.setInt(1, 2);
                ps.setString(2, "b");
                insertedRows = ps.executeUpdate();
                if (!Compatibility.skipDriverBugLowerThen("0.4.1")) {
                    Assert.assertEquals(1, insertedRows, "should insert 1 rows");
                }
            }
            Assert.assertEquals(countTable(s, "t1"), 2);

            // Now try to insert again with select
            try (PreparedStatement ps = conn.prepareStatement(insertSql)) {
                ps.setString(1, "a");
                int insertedRows = ps.executeUpdate();
                Assert.assertEquals(1, insertedRows, "should insert 1 row from the select");
            }
            Assert.assertEquals(countTable(s, "t1"), 3);
        }
    }

    @Test(groups = "IT")
    public void testMultiStatement() throws SQLException {
        if (Compatibility.skipDriverBugLowerThen("0.4.1")) {
            return;
        }
        try (Connection conn = getConn()) {
            assertThrows(SQLException.class, () -> conn.prepareStatement("select 1; select 1"));
        }
    }

    @Test(groups = "IT")
    public void testBatchAndNoBatch() throws SQLException {
        if (Compatibility.skipDriverBugLowerThen("0.4.1")) {
            return;
        }
        try (Connection conn = getConn();
             Statement s = conn.createStatement()) {
            s.execute("create or replace table t1(a int, b string)");
            String insertSql = "insert into t1 values (?, ?)";
            try (PreparedStatement ps = conn.prepareStatement(insertSql)) {
                ps.setInt(1, 1);
                ps.setString(2, "v1");
                ps.addBatch();

                ps.setInt(1, 2);
                ps.setString(2, "v2");
                int insertedRows = ps.executeUpdate();
                Assert.assertEquals(1, insertedRows);

                insertedRows = ps.executeUpdate();
                Assert.assertEquals(1, insertedRows);

                try(ResultSet rs = s.executeQuery("select * from t1")) {
                    for (int i = 0; i < 2; i++) {
                        assertTrue(rs.next());
                        assertEquals(rs.getInt(1) ,2);
                        assertEquals(rs.getString(2) ,"v2");
                    }
                    assertFalse(rs.next());
                }
                s.execute("truncate table t1");
                assertEquals(countTable(s, "t1"), 0);

                int[] counts = ps.executeBatch();
                Assert.assertEquals(counts, new int[] {1});

                try(ResultSet rs = s.executeQuery("select * from t1")) {
                    assertTrue(rs.next());
                    assertEquals(rs.getInt(1) ,1);
                    assertEquals(rs.getString(2) ,"v1");
                    assertFalse(rs.next());
                }
            }
        }
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
            Assert.assertFalse(r.next());
        }
    }
}
