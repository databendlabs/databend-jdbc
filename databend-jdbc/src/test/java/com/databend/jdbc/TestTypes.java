package com.databend.jdbc;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.sql.*;
import java.time.OffsetDateTime;
import java.util.Calendar;
import java.util.TimeZone;

public class TestTypes {

    @Test(groups = {"IT"})
    public void testTypeTimestampSameTimezone()
            throws SQLException {
        try (Connection connection = Utils.createConnectionWithPresignedUrlDisable();
             Statement statement = connection.createStatement()) {

            statement.execute("set timezone='Asia/Shanghai'");
            TimeZone targetZone = TimeZone.getTimeZone("Asia/Shanghai'");
            TimeZone.setDefault(targetZone);
            Calendar cal = Calendar.getInstance(targetZone);

            ResultSet r;
            Timestamp exp;

            Object[][] cases  = new Object[][] {
                    // use tz in string, ignore session timezone
                    {"2021-07-12T21:30:55+0700", "2021-07-12 14:30:55.000"},
                    // use tz in string, ignore session timezone
                    {"2022-07-12 14:30:55Z", "2022-07-12 14:30:55.000"},
                    {"2023-07-12 14:30:55", "2023-07-12 06:30:55.000"},
            };

            for (int i=0;i< cases.length; i++) {
                String sql = String.format("SELECT '%s'::timestamp", cases[i][0]);
                exp = Timestamp.valueOf((String) cases[i][1]);
                r = statement.executeQuery(sql);
                r.next();
                Assert.assertEquals(exp.toInstant(), r.getTimestamp(1).toInstant());
                Assert.assertEquals(exp, r.getTimestamp(1, cal));
                r.close();
            }

            Timestamp ts = Timestamp.valueOf("2021-01-12 14:30:55.123");
            statement.execute("create or replace table test_ts (a timestamp)");
            PreparedStatement ps = connection.prepareStatement("insert into test_ts values (?)");

            // without batch
            ps.setTimestamp(1, ts);
            ps.execute();
            r = statement.executeQuery("select * from test_ts");
            r.next();
            Assert.assertEquals(ts, r.getTimestamp(1));

            // with batch
            statement.execute("create or replace table test_ts (a timestamp)");
            ps.setTimestamp(1, ts);
            ps.addBatch();
            ps.executeBatch();
            r = statement.executeQuery("select * from test_ts");
            r.next();
            Assert.assertEquals(ts, r.getTimestamp(1));

            r.close();
        }
    }

    @Test(groups = {"IT"})
    public void testTypeTimestamp()
            throws SQLException {
        if (Compatibility.skipBugLowerThenOrEqualTo("1.2.844", "0.4.2")) {
            return;
        }
        try (Connection connection = Utils.createConnectionWithPresignedUrlDisable();
             Statement statement = connection.createStatement()) {
            statement.execute("set timezone='America/Los_Angeles'");

            TimeZone tz = TimeZone.getTimeZone("Europe/Moscow"); // +0300
            Calendar cal = Calendar.getInstance(tz);

            ResultSet r;
            Timestamp exp;

            Object[][] cases  = new Object[][] {
                // use tz in string, ignore session timezone
                {"2021-07-12T21:30:55+0700", "2021-07-12 14:30:55.000"},
                // use tz in string, ignore session timezone
                {"2022-07-12 14:30:55Z", "2022-07-12 14:30:55.000"},
                // use session timezone: PDT，Pacific Daylight Time：UTC-07:00
                {"2023-07-12 14:30:55", "2023-07-12 21:30:55.000"},
                // use session timezone:PST，Pacific Standard Time：UTC-08:00
                {"2024-01-12 14:30:55", "2024-01-12 22:30:55.000"}
            };

            for (int i=0;i< cases.length; i++) {
                String sql = String.format("SELECT '%s'::timestamp", cases[i][0]);
                exp = Timestamp.valueOf((String) cases[i][1]);
                r = statement.executeQuery(sql);
                r.next();
                Assert.assertEquals(exp, r.getTimestamp(1));
                // ignore the cal
                Assert.assertEquals(exp, r.getTimestamp(1, cal));
                r.close();
            }

            Timestamp ts = Timestamp.valueOf("2021-01-12 14:30:55.123");
            statement.execute("create or replace table test_ts (a timestamp)");
            PreparedStatement ps = connection.prepareStatement("insert into test_ts values (?)");

            // without batch
            ps.setTimestamp(1, ts);
            ps.execute();
            r = statement.executeQuery("select * from test_ts");
            r.next();
            Assert.assertEquals(ts, r.getTimestamp(1));

            // with batch
            statement.execute("create or replace table test_ts (a timestamp)");
            ps.setTimestamp(1, ts);
            ps.addBatch();
            ps.executeBatch();
            r = statement.executeQuery("select * from test_ts");
            r.next();
            Assert.assertEquals(ts, r.getTimestamp(1));

            r.close();
        }
    }

    @Test(groups = {"IT"})
    public void testTypeTimestampTz()
            throws SQLException {
        if (Compatibility.skipBugLowerThenOrEqualTo("1.2.844", "0.4.2")) {
            return;
        }
        try (Connection connection = Utils.createConnection();
             Statement statement = connection.createStatement()) {
            statement.execute("set timezone='America/Los_Angeles'");
            ResultSet r;

            r = statement.executeQuery("SELECT '1983-07-12 21:30:55 +0700'::timestamp_tz");
            r.next();
            Assert.assertEquals(r.getTimestamp(1), Timestamp.valueOf("1983-07-12 14:30:55.000"));
            r.close();

            statement.execute("create or replace table test_ts_tz (a timestamp_tz)");
            PreparedStatement ps = connection.prepareStatement("insert into test_ts_tz values (?)");
            // without batch
            ps.setObject(1, "2021-01-12T14:30:55.123+03:00");
            ps.execute();
            Timestamp exp = Timestamp.valueOf("2021-01-12 11:30:55.123");
            r = statement.executeQuery("select * from test_ts_tz");
            r.next();

            Assert.assertEquals(exp, r.getTimestamp(1));
        }
    }
}
