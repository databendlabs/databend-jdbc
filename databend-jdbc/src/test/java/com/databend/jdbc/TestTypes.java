package com.databend.jdbc;

import org.testng.Assert;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.sql.*;
import java.time.Instant;
import java.util.Calendar;
import java.util.TimeZone;

public class TestTypes {
    @BeforeSuite
    public void setTimeZone() {
        TimeZone.setDefault(TimeZone.getTimeZone("Asia/Shanghai"));
        System.out.println("=== setup timezone to AsiaShanghai ===");
    }

    @DataProvider(name = "flag")
    public Object[][] provideFlag() {
        return new Object[][]{
                {true,},
                {false,},
        };
    }

    @Test(groups = {"IT"}, dataProvider = "timezones")
    public void testGetTimestamp(boolean sameTZ)
            throws SQLException {
        String calTz;
        String sessionTz;
        if (sameTZ) {
            calTz = "Asia/Shanghai";
            sessionTz = "Asia/Shanghai";
        } else {
            if (Compatibility.skipBugLowerThenOrEqualTo("1.2.844", "0.4.2")) {
                return;
            }
            calTz = "Asia/Shanghai";
            sessionTz = "Europe/Moscow"; // +3
        }
        TimeZone targetZone = TimeZone.getTimeZone(calTz);
        Calendar cal = Calendar.getInstance(targetZone);


        try (Connection connection = Utils.createConnectionWithPresignedUrlDisable();
             Statement statement = connection.createStatement()) {

            statement.execute(String.format("set timezone='%s'", sessionTz));
            ResultSet r;
            Instant exp;

            Object[][] cases = new Object[][]{
                    // use tz in string, ignore session timezone
                    {"2021-07-12T21:30:55+0700", "2021-07-12T14:30:55.000Z"},
                    // use tz in string, ignore session timezone
                    {"2022-07-12 14:30:55Z", "2022-07-12T14:30:55.000Z"},
                    {"2023-07-12 14:30:55", "2023-07-12T06:30:55.000Z"},
            };

            for (int i = 0; i < cases.length; i++) {
                String sql = String.format("SELECT '%s'::timestamp", cases[i][0]);
                r = statement.executeQuery(sql);
                r.next();
                exp = Instant.parse((String) cases[i][1]);
                Assert.assertEquals(exp, r.getTimestamp(1).toInstant());
                Assert.assertEquals(exp, r.getTimestamp(1, cal).toInstant());
                r.close();
            }
        }
    }



    @Test(groups = {"IT"}, dataProvider = "flag")
    public void testSetTimestampSameTimeZone(boolean sameTZ)
            throws SQLException
    {
        if (Compatibility.skipDriverBugLowerThen( "0.4.3")) {
            return;
        }
        if (!sameTZ && Compatibility.skipServerBugLowerThen("1.2.844")) {
           return;
        }

        try (Connection connection = Utils.createConnectionWithPresignedUrlDisable();
             Statement statement = connection.createStatement()) {
            if (sameTZ) {
                statement.execute("set timezone='Asia/Shanghai'");
            } else {
                statement.execute("set timezone='America/Los_Angeles'");
            }

            Instant instant=  Instant.parse("2021-07-12T14:30:55.123Z");
            Timestamp ts = Timestamp.from(instant);
            Assert.assertEquals(ts, Timestamp.valueOf("2021-07-12 22:30:55.123"));

            statement.execute("create or replace table test_ts (a timestamp)");
            PreparedStatement ps = connection.prepareStatement("insert into test_ts values (?)");

            ResultSet r;
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
            Instant exp =  Instant.parse("1983-07-12 14:30:55.000Z");
            Assert.assertEquals(exp, r.getTimestamp(1).toInstant());
            r.close();

            statement.execute("create or replace table test_ts_tz (a timestamp_tz)");
            PreparedStatement ps = connection.prepareStatement("insert into test_ts_tz values (?)");
            // without batch
            ps.setObject(1, "2021-01-12T14:30:55.123+03:00");
            ps.execute();
            exp = Instant.parse("2021-01-12T11:30:55.123Z");
            r = statement.executeQuery("select * from test_ts_tz");
            r.next();

            Assert.assertEquals(exp, r.getTimestamp(1).toInstant());
        }
    }
}
