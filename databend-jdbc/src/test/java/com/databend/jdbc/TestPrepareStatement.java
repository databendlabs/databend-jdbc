package com.databend.jdbc;

import com.databend.client.StageAttachment;
import org.junit.jupiter.api.Assertions;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.*;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

public class TestPrepareStatement {
    private Connection createConnection()
            throws SQLException {
        String url = "jdbc:databend://localhost:8000?debug=true";
        return DriverManager.getConnection(url, "databend", "databend");
    }

    private Connection createConnection(boolean presignDisabled) throws SQLException {
        String url = "jdbc:databend://localhost:8000?presigned_url_disabled=" + presignDisabled;
        return DriverManager.getConnection(url, "databend", "databend");
    }

    @BeforeTest
    public void setUp()
            throws SQLException {
        // create table
        Connection c = createConnection();
        System.out.println("-----------------");
        System.out.println("drop all existing test table");
        c.createStatement().execute("drop table if exists test_prepare_statement");
        c.createStatement().execute("drop table if exists test_prepare_time");
        c.createStatement().execute("drop table if exists objects_test1");
        c.createStatement().execute("drop table if exists binary1");
        c.createStatement().execute("drop table if exists test_prepare_statement_null");
        c.createStatement().execute("create table test_prepare_statement (a int, b string)");
        c.createStatement().execute("create table test_prepare_statement_null (a int, b string)");
        c.createStatement().execute("create table test_prepare_time(a DATE, b TIMESTAMP)");
        // json data
        c.createStatement().execute("CREATE TABLE IF NOT EXISTS objects_test1(id TINYINT, obj VARIANT, d TIMESTAMP, s String, arr ARRAY(INT64)) Engine = Fuse");
        // Binary data
        c.createStatement().execute("create table IF NOT EXISTS binary1 (a binary);");
    }

    @Test(groups = "IT")
    public void TestBatchInsert() throws SQLException {
        Connection c = createConnection();
        c.setAutoCommit(false);

        PreparedStatement ps = c.prepareStatement("insert into test_prepare_statement values");
        ps.setInt(1, 1);
        ps.setString(2, "a");
        ps.addBatch();
        ps.setInt(1, 2);
        ps.setString(2, "b");
        ps.addBatch();
        System.out.println("execute batch insert");
        int[] ans = ps.executeBatch();
        Assert.assertEquals(ans.length, 2);
        Assert.assertEquals(ans[0], 1);
        Assert.assertEquals(ans[1], 1);
        Statement statement = c.createStatement();

        System.out.println("execute select");
        statement.execute("SELECT * from test_prepare_statement");
        ResultSet r = statement.getResultSet();

        while (r.next()) {
            System.out.println(r.getInt(1));
            System.out.println(r.getString(2));
        }
    }

    @Test(groups = "IT")
    public void TestBatchInsertWithNULL() throws SQLException {
        Connection c = createConnection();
        c.setAutoCommit(false);

        PreparedStatement ps = c.prepareStatement("insert into test_prepare_statement_null values");
        ps.setInt(1, 1);
        ps.setString(2, "");
        ps.addBatch();
        ps.setInt(1, 2);
        ps.setNull(2, Types.NULL);
        ps.addBatch();
        ps.setInt(1, 3);
        ps.setObject(2, null, Types.NULL);
        ps.addBatch();
        System.out.println("execute batch insert");
        int[] ans = ps.executeBatch();
        Assert.assertEquals(ans.length, 3);
        Assert.assertEquals(ans[0], 1);
        Assert.assertEquals(ans[1], 1);
        Statement statement = c.createStatement();

        System.out.println("execute select");
        statement.execute("SELECT * from test_prepare_statement_null");
        ResultSet r = statement.getResultSet();

        while (r.next()) {
            System.out.println(r.getInt(1));
            Assert.assertEquals(r.getObject(2), null);
        }
    }

    @Test(groups = "IT")
    public void TestConvertSQLWithBatchValues() throws SQLException {
        List<String[]> batchValues = new ArrayList<>();
        // Add string arrays to batchValues
        String[] values1 = {"1"};
        String[] values2 = {"2"};
        batchValues.add(values1);
        batchValues.add(values2);

        String originalSql = "delete from table where id = ?";
        String expectedSql = "delete from table where id = 1;\ndelete from table where id = 2;\n";
        Assert.assertEquals(DatabendPreparedStatement.convertSQLWithBatchValues(originalSql, batchValues), expectedSql);

        List<String[]> batchValues1 = new ArrayList<>();
        // Add string arrays to batchValues
        String[] values3 = {"1", "2"};
        String[] values4 = {"3", "4"};
        batchValues1.add(values3);
        batchValues1.add(values4);

        String originalSql1 = "delete from table where id = ? and uuid = ?";
        String expectedSql1 = "delete from table where id = 1 and uuid = 2;\ndelete from table where id = 3 and uuid = 4;\n";
        Assert.assertEquals(DatabendPreparedStatement.convertSQLWithBatchValues(originalSql1, batchValues1), expectedSql1);
    }

    @Test(groups = "IT")
    public void TestBatchDelete() throws SQLException {
        Connection c = createConnection();
        c.setAutoCommit(false);

        PreparedStatement ps = c.prepareStatement("insert into test_prepare_statement values");
        ps.setInt(1, 1);
        ps.setString(2, "b");
        ps.addBatch();
        ps.setInt(1, 3);
        ps.setString(2, "b");
        ps.addBatch();
        System.out.println("execute batch insert");
        int[] ans = ps.executeBatch();
        Assert.assertEquals(ans.length, 2);
        Assert.assertEquals(ans[0], 1);
        Assert.assertEquals(ans[1], 1);
        Statement statement = c.createStatement();

        System.out.println("execute select");
        statement.execute("SELECT * from test_prepare_statement");
        ResultSet r = statement.getResultSet();

        while (r.next()) {
            System.out.println(r.getInt(1));
            System.out.println(r.getString(2));
        }

        PreparedStatement deletePs = c.prepareStatement("delete from test_prepare_statement where a = ?");
        deletePs.setInt(1, 1);
        deletePs.addBatch();
        int[] ansDel = deletePs.executeBatch();
        System.out.println(ansDel);

        System.out.println("execute select");
        statement.execute("SELECT * from test_prepare_statement");
        ResultSet r1 = statement.getResultSet();

        int resultCount = 0;
        while (r1.next()) {
            resultCount += 1;
        }
        Assert.assertEquals(resultCount, 1);
    }

    @Test(groups = "IT")
    public void TestBatchInsertWithTime() throws SQLException {
        Connection c = createConnection();
        c.setAutoCommit(false);
        PreparedStatement ps = c.prepareStatement("insert into test_prepare_time values");
        ps.setDate(1, Date.valueOf("2020-01-10"));
        ps.setTimestamp(2, Timestamp.valueOf("1983-07-12 21:30:55.888"));
        ps.addBatch();
        ps.setDate(1, Date.valueOf("1970-01-01"));
        ps.setTimestamp(2, Timestamp.valueOf("1970-01-01 00:00:01"));
        ps.addBatch();
        ps.setDate(1, Date.valueOf("2021-01-01"));
        ps.setTimestamp(2, Timestamp.valueOf("1970-01-01 00:00:01.234"));
        int[] ans = ps.executeBatch();
        Statement statement = c.createStatement();

        System.out.println("execute select on time");
        statement.execute("SELECT * from test_prepare_time");
        ResultSet r = statement.getResultSet();

        while (r.next()) {
            System.out.println(r.getDate(1).toString());
            System.out.println(r.getTimestamp(2).toString());
        }
    }

    @Test(groups = "IT")
    public void TestBatchInsertWithComplexDataType() throws SQLException {
        Connection c = createConnection();
        c.setAutoCommit(false);
        PreparedStatement ps = c.prepareStatement("insert into objects_test1 values");
        ps.setInt(1, 1);
        ps.setString(2, "{\"a\": 1,\"b\": 2}");
        ps.setTimestamp(3, Timestamp.valueOf("1983-07-12 21:30:55.888"));
        ps.setString(4, "hello world, 你好");
        ps.setString(5, "[1,2,3,4,5]");
        ps.addBatch();
        int[] ans = ps.executeBatch();
        Statement statement = c.createStatement();

        System.out.println("execute select on object");
        statement.execute("SELECT * from objects_test1");
        ResultSet r = statement.getResultSet();

        while (r.next()) {
            System.out.println(r.getInt(1));
            System.out.println(r.getString(2));
            System.out.println(r.getTimestamp(3).toString());
            System.out.println(r.getString(4));
            System.out.println(r.getString(5));
        }
    }

    @Test(groups = "IT")
    public void TestBatchInsertWithComplexDataTypeWithPresignAPI() throws SQLException {
        Connection c = createConnection(true);
        c.setAutoCommit(false);
        PreparedStatement ps = c.prepareStatement("insert into objects_test1 values");
        ps.setInt(1, 1);
        ps.setString(2, "{\"a\": 1,\"b\": 2}");
        ps.setTimestamp(3, Timestamp.valueOf("1983-07-12 21:30:55.888"));
        ps.setString(4, "hello world, 你好");
        ps.setString(5, "[1,2,3,4,5]");
        ps.addBatch();
        int[] ans = ps.executeBatch();
        Statement statement = c.createStatement();

        System.out.println("execute select on object");
        statement.execute("SELECT * from objects_test1");
        ResultSet r = statement.getResultSet();

        while (r.next()) {
            System.out.println(r.getInt(1));
            System.out.println(r.getString(2));
            System.out.println(r.getTimestamp(3).toString());
            System.out.println(r.getString(4));
            System.out.println(r.getString(5));
        }
    }

    @Test(groups = "IT")
    public void TestBatchInsertWithComplexDataTypeWithPresignAPIPlaceHolder() throws SQLException {
        Connection c = createConnection(true);
        c.setAutoCommit(false);
        PreparedStatement ps = c.prepareStatement("insert into objects_test1 values(?,?,?,?,?)");
        for (int i = 0; i < 500000; i++) {
            ps.setInt(1, 2);
            ps.setString(2, "{\"a\": 1,\"b\": 2}");
            ps.setTimestamp(3, Timestamp.valueOf("1983-07-12 21:30:55.888"));
            ps.setString(4, "hello world, 你好");
            ps.setString(5, "[1,2,3,4,5]");
            ps.addBatch();
        }

        int[] ans = ps.executeBatch();
        Statement statement = c.createStatement();

        System.out.println("execute select on object");
        statement.execute("SELECT * from objects_test1");
        ResultSet r = statement.getResultSet();
        int count = 0;
        while (r.next()) {
            count++;
        }
        System.out.println(count);
    }

    @Test(groups = "IT")
    public void TestBatchReplaceInto() throws SQLException {
        Connection c = createConnection();
        c.setAutoCommit(false);
        PreparedStatement ps1 = c.prepareStatement("insert into test_prepare_statement values");
        ps1.setInt(1, 1);
        ps1.setInt(2, 2);
        ps1.addBatch();
        ps1.executeBatch();

        PreparedStatement ps = c.prepareStatement("replace into test_prepare_statement on(a) values");
        ps.setInt(1, 1);
        ps.setString(2, "a");
        ps.addBatch();
        ps.setInt(1, 3);
        ps.setString(2, "b");
        ps.addBatch();
        System.out.println("execute batch replace into");
        int[] ans = ps.executeBatch();
        Assert.assertEquals(ans.length, 2);
        Assert.assertEquals(ans[0], 1);
        Assert.assertEquals(ans[1], 1);
        Statement statement = c.createStatement();

        System.out.println("execute select");
        statement.execute("SELECT * from test_prepare_statement");
        ResultSet r = statement.getResultSet();

        while (r.next()) {
            System.out.println(r.getInt(1));
            System.out.println(r.getString(2));
        }
    }

    @Test
    public void testPrepareStatementExecute() throws SQLException {
        Connection conn = createConnection();
        conn.createStatement().execute("delete from test_prepare_statement");
        String insertSql = "insert into test_prepare_statement values (?,?)";
        try (PreparedStatement statement = conn.prepareStatement(insertSql)) {
            statement.setInt(1, 1);
            statement.setString(2, "b");
            statement.execute();
        }
        String updateSql = "update test_prepare_statement set b = ? where a = ?";
        try (PreparedStatement statement = conn.prepareStatement(updateSql)) {
            statement.setString(1, "c");
            statement.setInt(2, 1);
            statement.execute();
        }

        String selectSql = "select * from test_prepare_statement";
        try {
            Statement statement = conn.createStatement();
            ResultSet rs = statement.executeQuery(selectSql);
            while (rs.next()) {
                Assert.assertEquals("c", rs.getString(2));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        String deleteSql = "delete from test_prepare_statement where a = ?";
        try (PreparedStatement statement = conn.prepareStatement(deleteSql)) {
            statement.setInt(1, 1);
            statement.execute();
        }

        try {
            Statement statement = conn.createStatement();
            ResultSet rs = statement.executeQuery(selectSql);
            Assert.assertEquals(0, rs.getRow());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testUpdateSetNull() throws SQLException {
        Connection conn = createConnection();
        String sql = "insert into test_prepare_statement values (?,?)";
        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setInt(1, 1);
            statement.setString(2, "b");
            statement.addBatch();
            int[] result = statement.executeBatch();
            System.out.println(result);
            Assertions.assertEquals(1, result.length);
        }
        String updateSQL = "update test_prepare_statement set b = ? where a = ?";
        try (PreparedStatement statement = conn.prepareStatement(updateSQL)) {
            statement.setInt(2, 1);
            statement.setNull(1, Types.NULL);
            int result = statement.executeUpdate();
            System.out.println(result);
            Assertions.assertEquals(2, result);
        }
        try (PreparedStatement statement = conn.prepareStatement("select a, regexp_replace(b, '\\d', '*') from test_prepare_statement where a = ?")) {
            statement.setInt(1, 1);
            ResultSet r = statement.executeQuery();
            while (r.next()) {
                Assertions.assertEquals(1, r.getInt(1));
                Assertions.assertEquals(null, r.getString(2));
            }
        }
        String insertSelectSql = "insert overwrite test_prepare_statement select * from test_prepare_statement";
        try (PreparedStatement statement = conn.prepareStatement(insertSelectSql)) {
            statement.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testUpdateStatement() throws SQLException {
        Connection conn = createConnection();
        String sql = "insert into test_prepare_statement values (?,?)";
        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setInt(1, 1);
            statement.setString(2, "b");
            statement.addBatch();
            int[] result = statement.executeBatch();
            System.out.println(result);
            Assertions.assertEquals(1, result.length);
        }
        String updateSQL = "update test_prepare_statement set b = ? where a = ?";
        try (PreparedStatement statement = conn.prepareStatement(updateSQL)) {
            statement.setInt(2, 1);
            statement.setObject(1, "c'c");
            int result = statement.executeUpdate();
            System.out.println(result);
            Assertions.assertEquals(2, result);
        }
        try (PreparedStatement statement = conn.prepareStatement("select a, regexp_replace(b, '\\d', '*') from test_prepare_statement where a = ?")) {
            statement.setInt(1, 1);
            ResultSet r = statement.executeQuery();
            while (r.next()) {
                Assertions.assertEquals(1, r.getInt(1));
                Assertions.assertEquals("c'c", r.getString(2));
            }
        }
    }

    @Test
    public void testAllPreparedStatement() throws SQLException {
        String sql = "insert into test_prepare_statement values (?,?)";
        Connection conn = createConnection();
        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setInt(1, 1);
            statement.setString(2, "b");
            statement.addBatch();
            int[] result = statement.executeBatch();
            System.out.println(result);
            Assertions.assertEquals(1, result.length);
        }
        String updateSQL = "update test_prepare_statement set b = ? where a = ?";
        try (PreparedStatement statement = conn.prepareStatement(updateSQL)) {
            statement.setInt(2, 1);
            statement.setString(1, "c");
            int result = statement.executeUpdate();
            System.out.println(result);
            Assertions.assertEquals(2, result);
        }
        try (PreparedStatement statement = conn.prepareStatement("select a, regexp_replace(b, '\\d', '*') from test_prepare_statement where b = ?")) {
            statement.setString(1, "c");
            ResultSet r = statement.executeQuery();
            while (r.next()) {
                Assertions.assertEquals(1, r.getInt(1));
                Assertions.assertEquals("c", r.getString(2));
            }
        }
        String replaceIntoSQL = "replace into test_prepare_statement on(a) values (?,?)";
        try (PreparedStatement statement = conn.prepareStatement(replaceIntoSQL)) {
            statement.setInt(1, 1);
            statement.setString(2, "d");
            statement.addBatch();
            int[] result = statement.executeBatch();
        }
        ResultSet r2 = conn.createStatement().executeQuery("select * from test_prepare_statement");
        while (r2.next()) {
            System.out.println(r2.getInt(1));
            System.out.println(r2.getString(2));
        }

        String deleteSQL = "delete from test_prepare_statement where a = ?";
        try (PreparedStatement statement = conn.prepareStatement(deleteSQL)) {
            statement.setInt(1, 1);
            boolean result = statement.execute();
            System.out.println(result);
        }

        String deleteSQLVarchar = "delete from test_prepare_statement where b = ?";
        try (PreparedStatement statement = conn.prepareStatement(deleteSQLVarchar)) {
            statement.setString(1, "1");
            int result = statement.executeUpdate();
            System.out.println(result);
        }

        ResultSet r3 = conn.createStatement().executeQuery("select * from test_prepare_statement");
        Assert.assertEquals(0, r3.getRow());
        while (r3.next()) {
            // noting print
            System.out.println(r3.getInt(1));
            System.out.println(r3.getString(2));
        }
    }

    //TODO(hantmac): fix this test case
//    @Test
//    public void testSetBlobNotNull() throws SQLException {
//        String sql = "insert into binary1 values (?)";
//        Connection conn = createConnection();
//        // Create a Blob
//        String blobData = "blob data";
//        InputStream blobInputStream = new ByteArrayInputStream(blobData.getBytes());
//        try (PreparedStatement statement = conn.prepareStatement(sql)) {
//            statement.setBlob(1, blobInputStream);
//            statement.addBatch();
//            int[] result = statement.executeBatch();
//            System.out.println(result);
//            Assertions.assertEquals(1, result.length);
//        }
//    }

    @Test
    public void shouldBuildStageAttachmentWithFileFormatOptions() throws SQLException {
        Connection conn = createConnection();
        Assertions.assertEquals("", conn.unwrap(DatabendConnection.class).binaryFormat());
        StageAttachment stageAttachment = DatabendPreparedStatement.buildStateAttachment((DatabendConnection) conn, "stagePath");

        Assertions.assertFalse(stageAttachment.getFileFormatOptions().containsKey("binary_format"));
        Assertions.assertTrue(stageAttachment.getFileFormatOptions().containsKey("type"));
        Assertions.assertEquals("true", stageAttachment.getCopyOptions().get("PURGE"));
        Assertions.assertEquals("\\N", stageAttachment.getCopyOptions().get("NULL_DISPLAY"));
    }

    @Test(groups = {"IT", "FLAKY"})
    public void testSelectWithClusterKey() throws SQLException {
        Connection conn = createConnection();
        conn.createStatement().execute("drop table if exists default.test_clusterkey");
        conn.createStatement().execute("create table default.test_clusterkey (a int, b string)");
        String insertSql = "insert into default.test_clusterkey values (?,?)";
        try (PreparedStatement statement = conn.prepareStatement(insertSql)) {
            statement.setInt(1, 1);
            statement.setString(2, "b");
            statement.addBatch();
            statement.setInt(1, 2);
            statement.setString(2, "c");
            statement.addBatch();
            int[] result = statement.executeBatch();
            System.out.println(result);
            Assertions.assertEquals(2, result.length);
        }
        conn.createStatement().execute("alter table default.test_clusterkey cluster by (a)");
        String selectSQL = "select * from clustering_information('default','test_clusterkey')";
        try (PreparedStatement statement = conn.prepareStatement(selectSQL)) {
            ResultSet rs = statement.executeQuery();
            while (rs.next()) {
                Assertions.assertEquals("0.0", rs.getString(5));
            }
        }
    }

    @Test
    public void testEncodePass() throws SQLException {
        Connection conn = createConnection();
        conn.createStatement().execute("create user if not exists 'u01' identified by 'mS%aFRZW*GW';");
        conn.createStatement().execute("GRANT ALL PRIVILEGES ON default.* TO 'u01'@'%'");

        Connection conn2 = DriverManager.getConnection("jdbc:databend://localhost:8000", "u01", "mS%aFRZW*GW");
        conn2.createStatement().execute("select 1");
        conn.createStatement().execute("drop user if exists 'u01'");
    }
}
