package com.databend.jdbc;

import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.UUID;

@Test(timeOut = 10000)
public class TestQueryResultFormatConsistency {
    private static final String TEST_QUERY_RESULT_FORMAT = "DATABEND_JDBC_TEST_QUERY_RESULT_FORMAT";

    @Test(groups = {"IT"})
    public void testJsonAndArrowReturnConsistentValues() throws Exception {
        String configuredFormat = System.getenv(TEST_QUERY_RESULT_FORMAT);
        if (configuredFormat == null || !"arrow".equalsIgnoreCase(configuredFormat.trim())) {
            throw new SkipException("Set DATABEND_JDBC_TEST_QUERY_RESULT_FORMAT=arrow to run Arrow consistency checks");
        }

        String dbName = ("format_consistency_" + UUID.randomUUID()).replace("-", "").toLowerCase();
        try (Connection admin = Utils.createConnection();
             Statement statement = admin.createStatement()) {
            statement.execute("create or replace database " + dbName);
            statement.execute("create or replace table " + dbName + ".t_consistency (id int, amount decimal(10,2), created_at timestamp, note string null)");
            statement.execute("insert into " + dbName + ".t_consistency values (1, 123.45, '2024-04-16 12:34:56.123456', null)");
        }

        try (Connection jsonConnection = createConnectionWithFormat(dbName, "json");
             Connection arrowConnection = createConnectionWithFormat(dbName, "arrow")) {
            RowSnapshot jsonRow = fetchRow(jsonConnection);
            RowSnapshot arrowRow = fetchRow(arrowConnection);

            Assert.assertEquals(arrowRow.id, jsonRow.id);
            Assert.assertEquals(arrowRow.amount, jsonRow.amount);
            Assert.assertEquals(arrowRow.createdAtText, jsonRow.createdAtText);
            Assert.assertEquals(arrowRow.note, jsonRow.note);
            Assert.assertEquals(arrowRow.amountType, jsonRow.amountType);
            Assert.assertEquals(arrowRow.timestampType, jsonRow.timestampType);
        }
    }

    private static Connection createConnectionWithFormat(String database, String format) throws SQLException {
        Properties props = new Properties();
        props.setProperty("user", Utils.getUsername());
        props.setProperty("password", Utils.getPassword());
        props.setProperty("query_result_format", format);
        return Utils.createConnection(database, props);
    }

    private static RowSnapshot fetchRow(Connection connection) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "select id, amount, created_at, note from t_consistency order by id");
             ResultSet rs = statement.executeQuery()) {
            Assert.assertTrue(rs.next());
            ResultSetMetaData metaData = rs.getMetaData();
            RowSnapshot snapshot = new RowSnapshot();
            snapshot.id = rs.getInt(1);
            snapshot.amount = rs.getBigDecimal(2).stripTrailingZeros().toPlainString();
            snapshot.createdAtText = rs.getString(3);
            snapshot.note = rs.getString(4);
            snapshot.amountType = metaData.getColumnType(2);
            snapshot.timestampType = metaData.getColumnType(3);
            Assert.assertFalse(rs.next());
            return snapshot;
        }
    }

    private static final class RowSnapshot {
        private int id;
        private String amount;
        private String createdAtText;
        private String note;
        private int amountType;
        private int timestampType;
    }
}
