package com.databend.jdbc;

import com.vdurmont.semver4j.Semver;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import org.testng.Assert;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.sql.*;
import java.time.*;
import java.util.Calendar;
import java.util.TimeZone;

import static com.databend.jdbc.Compatibility.driverVersion;

public class TestTypes {
    @BeforeSuite(alwaysRun = true)
    public void beforeSuite() {
        TimeZone.setDefault(TimeZone.getTimeZone("Asia/Shanghai"));
        System.out.println("=== setup timezone to AsiaShanghai beforeSuite ===");
    }

    @DataProvider(name = "flag")
    public Object[][] provideFlag() {
        return new Object[][]{
                {true,},
                {false,},
        };
    }

    @Test(groups = {"IT"}, dataProvider = "flag")
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
            calTz = "America/Los_Angeles"; // -8/-7
            sessionTz = "Europe/Moscow"; // +3
        }
        TimeZone targetZone = TimeZone.getTimeZone(calTz);
        Calendar cal = Calendar.getInstance(targetZone);
        System.out.println("testGetTimestamp sameTZ=" + sameTZ);


        try (Connection connection = Utils.createConnection();
             Statement statement = connection.createStatement()) {

            statement.execute(String.format("set timezone='%s'", sessionTz));

            Object[][] cases = new Object[][]{
                    // use tz in string, ignore session timezone
                    {"2021-07-12T21:30:55+0700", "2021-07-12T14:30:55.000Z", "same"},
                    // use tz in string, ignore session timezone
                    {"2022-07-12 14:30:55Z", "2022-07-12T14:30:55.000Z", "same"},
                    {"2023-07-12 14:30:55", "2023-07-12T06:30:55.000Z", "2023-07-12T11:30:55.000Z"},
            };

            for (int i = 0; i < cases.length; i++) {
                String sql = String.format("SELECT '%s'::timestamp", cases[i][0]);
                ResultSet r = statement.executeQuery(sql);
                r.next();
                String expS;
                if (sameTZ) {
                    expS = (String) cases[i][1];
                } else {
                    expS = (String) cases[i][2];
                    if (expS.equals("same")) {
                        expS = (String) cases[i][1];
                    }
                }
                Instant exp = Instant.parse(expS);

                if (driverVersion == null || driverVersion.isGreaterThanOrEqualTo(new Semver("0.4.3"))) {
                    // recommended
                    ZonedDateTime z = r.getObject(1, ZonedDateTime.class);
                    Assert.assertEquals(exp, z.toInstant());

                    // recommended
                    Instant instant = r.getObject(1, Instant.class);
                    Assert.assertEquals(exp, instant);

                    OffsetDateTime o = r.getObject(1, OffsetDateTime.class);
                    Assert.assertEquals(exp, o.toInstant());
                }

                Assert.assertEquals(exp, r.getTimestamp(1).toInstant());
                // ignore cal
                Assert.assertEquals(exp, r.getTimestamp(1, cal).toInstant());

                r.close();
            }
        }
    }


    @Test(groups = {"IT"})
    public void testGetTimestampTz()
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
            Instant exp = Instant.parse("1983-07-12T14:30:55.000Z");

            // recommended
            OffsetDateTime offsetDateTime = r.getObject(1, OffsetDateTime.class);
            Assert.assertEquals(exp.atOffset(ZoneOffset.ofHours(7)), offsetDateTime);

            Assert.assertEquals(exp, r.getTimestamp(1).toInstant());

            Assert.assertEquals(exp, r.getObject(1, Instant.class));
            r.close();
        }
    }


    @Test(groups = {"IT"}, dataProvider = "flag")
    public void testSetTimestamp(boolean withTz)
            throws SQLException {
        if (Compatibility.skipBugLowerThenOrEqualTo("1.2.844", "0.4.2")) {
            return;
        }
        try (Connection connection = Utils.createConnection();
             Statement statement = connection.createStatement()) {
            statement.execute("set timezone='America/Los_Angeles'");
            ResultSet r;

            if (withTz) {
                statement.execute("create or replace table test_ts_tz (a timestamp_tz, b int)");
            } else {
                statement.execute("create or replace table test_ts_tz (a timestamp, b int)");
            }
            PreparedStatement ps = connection.prepareStatement("insert into test_ts_tz values (?, ?)");

            String timeStringNoTZ = "2021-01-12T14:30:55.123";
            String timeString = timeStringNoTZ + "+03:00";
            OffsetDateTime offsetDateTime = OffsetDateTime.parse(timeString);
            ZonedDateTime zonedDateTime = LocalDateTime.parse(timeStringNoTZ).atZone(ZoneId.of("Europe/Moscow"));
            Instant instant = offsetDateTime.toInstant();
            Timestamp timestamp = Timestamp.from(instant);
            Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("Europe/Moscow"));

            // 1, 2, 3, 4: same epoch, use tz in parameter,
            ps.setObject(1, offsetDateTime);
            ps.setInt(2, 1);
            ps.execute();

            ps.setObject(1, zonedDateTime);
            ps.setInt(2, 2);
            ps.execute();

            ps.setString(1, timeString);
            ps.setInt(2, 3);
            ps.execute();

            ps.setTimestamp(1, timestamp, cal);
            ps.setInt(2, 4);
            ps.execute();

            // 5, 6: same epoch, tz=UTC
            ps.setObject(1, instant);
            ps.setInt(2, 5);
            ps.execute();

            ps.setTimestamp(1, timestamp);
            ps.setInt(2, 6);
            ps.execute();

            // 7, 8: diff epoch, set use session tz
            ps.setString(1, timeStringNoTZ);
            ps.setInt(2, 7);
            ps.execute();

            ps.setObject(1, LocalDateTime.parse(timeStringNoTZ));
            ps.setInt(2, 8);
            ps.execute();

            r = statement.executeQuery("select a from test_ts_tz order by b");

            ZoneId sessionZoneId = ZoneId.of("America/Los_Angeles");
            ZoneOffset sessionOffset = sessionZoneId.getRules().getOffset(instant);
            OffsetDateTime exp = offsetDateTime;
            if (!withTz) {
                exp = instant.atOffset(sessionOffset);
            }

            // 1, 2, 3, 4, 5: same epoch, use tz in parameter,
            r.next();
            Assert.assertEquals(exp, r.getObject(1, OffsetDateTime.class));
            if (!withTz) {
                Assert.assertEquals(instant.atZone(sessionZoneId), r.getObject(1, ZonedDateTime.class));
            }
            r.next();
            Assert.assertEquals(exp, r.getObject(1, OffsetDateTime.class));
            r.next();
            Assert.assertEquals(exp, r.getObject(1, OffsetDateTime.class));
            r.next();
            Assert.assertEquals(exp, r.getObject(1, OffsetDateTime.class));

            OffsetDateTime o;

            // 5, 6: set use tz=UTC
            int expOffsetForUTC = withTz? 0: -8 * 3600;
            r.next();
            o = r.getObject(1, OffsetDateTime.class);
            Assert.assertEquals(o.toInstant(), offsetDateTime.toInstant());
            Assert.assertEquals(o.getOffset().getTotalSeconds(), expOffsetForUTC);

            r.next();
            o = r.getObject(1, OffsetDateTime.class);
            Assert.assertEquals(o.toInstant(), offsetDateTime.toInstant());
            Assert.assertEquals(o.getOffset().getTotalSeconds(), expOffsetForUTC);

            // 7, 8: set use session tz
            r.next();
            o = r.getObject(1, OffsetDateTime.class);
            Assert.assertEquals(o.toInstant(), Instant.parse("2021-01-12T22:30:55.123Z"));
            Assert.assertEquals(o.getOffset().getTotalSeconds(), -8 * 3600);

            r.next();
            o = r.getObject(1, OffsetDateTime.class);
            Assert.assertEquals(o.toInstant(), Instant.parse("2021-01-12T22:30:55.123Z"));
            Assert.assertEquals(o.getOffset().getTotalSeconds(), -8 * 3600);

            r.close();
        }
    }

    @DataProvider(name = "timeZoneAndFlag")
    public Object[][] provideTimeZoneAndFlag() {
        return new Object[][]{
                {"Asia/Shanghai", true},
                {"Asia/Tokyo", true},
                {"America/Los_Angeles", true},
                {"Asia/Shanghai", false},
                {"Asia/Tokyo", false},
                {"America/Los_Angeles", false}
        };
    }

    @Test(groups = "IT", dataProvider = "timeZoneAndFlag")
    public void TestDate(String tz, boolean useLocalDate) throws SQLException {
        if (Compatibility.skipBugLowerThenOrEqualTo("1.2.844", "0.4.2")) {
            return;
        }

        try (Connection c = Utils.createConnection();
             Statement s = c.createStatement()) {

            s.execute("create or replace table t1(a DATE)");
            s.execute(String.format("set timezone='%s'", tz));

            String dateStr = "2020-01-10";
            Date date = Date.valueOf(dateStr);
            LocalDate localDate = LocalDate.of(2020, 1, 10);
            Assert.assertEquals(date.toLocalDate(), localDate);
            Assert.assertEquals(Date.valueOf(localDate), date);


            PreparedStatement ps = c.prepareStatement("insert into t1 values (?)");
            if (useLocalDate) {
                ps.setObject(1, localDate);
            } else {
                ps.setDate(1, date);
            }
            ps.execute();

            s.execute("SELECT * from t1");
            ResultSet r = s.getResultSet();

            Assert.assertTrue(r.next());
            Assert.assertEquals(r.getDate(1), date);
            Assert.assertEquals(r.getString(1), dateStr);
            Assert.assertEquals(r.getObject(1, LocalDate.class), localDate);

            Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("America/Los_Angeles"));
            Assert.assertEquals(r.getDate(1, cal).toLocalDate(), date.toLocalDate());

            // 2020-01-10 00:00 in tokyo is 2020-01-09 23:00 in Shanghai
            cal = Calendar.getInstance(TimeZone.getTimeZone("Asia/Tokyo"));
            Assert.assertEquals(r.getDate(1, cal).toLocalDate(), LocalDate.of(2020, 1, 9));

            Assert.assertFalse(r.next());
        }
    }

    @Test(groups = {"IT"})
    public void TestInterval() throws SQLException {
        try (Connection c = Utils.createConnection();
             Statement s = c.createStatement()) {

            s.execute("create or replace table test_interval(a interval)");

            PreparedStatement ps = c.prepareStatement("insert into test_interval values (?)");
            String interval = "-3 days";
            ps.setString(1, interval);
            ps.execute();

            s.execute("SELECT * from test_interval");
            ResultSet r = s.getResultSet();

            Assert.assertTrue(r.next());
            Assert.assertEquals(r.getString(1), interval);

            Assert.assertFalse(r.next());
        }
    }

    @Test(groups = {"IT"})
    public void testSelectGeometry() throws SQLException, ParseException {
        try (Connection connection = Utils.createConnection();
             Statement statement = connection.createStatement()
        ) {
            statement.execute("set enable_geo_create_table=1");
            statement.execute("CREATE or replace table cities ( id INT, name VARCHAR NOT NULL, location GEOMETRY);");
            statement.execute("INSERT INTO cities (id, name, location) VALUES (1, 'New York', 'POINT (-73.935242 40.73061))');");
            statement.execute("INSERT INTO cities (id, name, location) VALUES (2, 'Null', null);");
            try (ResultSet r = statement.executeQuery("select location from cities order by id")) {
                r.next();
                Assert.assertEquals("{\"type\": \"Point\", \"coordinates\": [-73.935242,40.73061]}", r.getObject(1));
                r.next();
                Assert.assertNull(r.getObject(1));
            }

            // set geometry_output_format to wkb
            connection.createStatement().execute("set geometry_output_format='WKB'");
            try (ResultSet r = statement.executeQuery("select location from cities order by id")) {
                r.next();
                byte[] wkb = r.getBytes(1);
                WKBReader wkbReader = new WKBReader();
                Geometry geometry = wkbReader.read(wkb);
                Assert.assertEquals("POINT (-73.935242 40.73061)", geometry.toText());
            }
        }
    }
}
