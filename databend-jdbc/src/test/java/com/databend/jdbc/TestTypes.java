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

            for (Object[] aCase : cases) {
                String sql = String.format("SELECT '%s'::timestamp", aCase[0]);
                ResultSet r = statement.executeQuery(sql);
                r.next();
                String expS;
                if (sameTZ) {
                    expS = (String) aCase[1];
                } else {
                    expS = (String) aCase[2];
                    if (expS.equals("same")) {
                        expS = (String) aCase[1];
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
        if (Compatibility.skipDriverBugLowerThen("0.4.2")) {
            return;
        }
        try (Connection c = Utils.createConnection();
             Statement s = c.createStatement()) {

            s.execute("create or replace table test_interval(id int, a interval)");

            Object[][] cases = new Object[][]{
                    {1, "-3 days", "-3 days", Duration.ofDays(-3), false},
                    {2, Duration.ZERO, "00:00:00", Duration.ZERO, false},
                    {3, "1 day 2:30:15", "1 day 2:30:15", Duration.ofDays(1).plusHours(2).plusMinutes(30).plusSeconds(15), false},
                    {4, Duration.ofHours(10).plusMinutes(15).plusSeconds(30).negated(), "-10:15:30", Duration.ofHours(10).plusMinutes(15).plusSeconds(30).negated(), true},
                    {5, Duration.ofHours(4).plusMinutes(5).plusSeconds(6).plusNanos(7000), "4:05:06.000007", Duration.ofHours(4).plusMinutes(5).plusSeconds(6).plusNanos(7_000), false},
                    {6, Duration.ofDays(1).plusHours(2).plusMinutes(3).plusSeconds(4), "26:03:04", Duration.ofDays(1).plusHours(2).plusMinutes(3).plusSeconds(4), false}
            };

            try (PreparedStatement ps = c.prepareStatement("insert into test_interval values (?, ?)")) {
                for (Object[] testCase : cases) {
                    ps.setInt(1, (Integer) testCase[0]);
                    Object param = testCase[1];
                    boolean useExplicitType = (Boolean) testCase[4];
                    if (param instanceof Duration) {
                        if (useExplicitType) {
                            ps.setObject(2, param, Types.VARCHAR);
                        } else {
                            ps.setObject(2, param);
                        }
                    } else {
                        ps.setString(2, (String) param);
                    }
                    ps.execute();
                }
            }

            try (ResultSet r = s.executeQuery("SELECT id, a FROM test_interval ORDER BY id")) {
                for (Object[] testCase : cases) {
                    Assert.assertTrue(r.next());
                    Assert.assertEquals(r.getInt(1), testCase[0]);

                    Assert.assertEquals(r.getObject(2, Duration.class), testCase[3]);

                    Assert.assertEquals(r.getString(2), testCase[2]);

                    Object defaultValue = r.getObject(2);
                    Assert.assertTrue(defaultValue instanceof Duration);
                    Assert.assertEquals(defaultValue, testCase[3]);
                }
                Assert.assertFalse(r.next());
            }
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

    @Test(groups = {"IT"}, dataProvider = "flag")
    public void TestBatchInsertWithNULL(boolean batch) throws SQLException {
        if (Compatibility.skipDriverBugLowerThen("0.4.3")) {
            return;
        }
        try (Connection c = Utils.createConnectionWithPresignedUrlDisable();
             Statement s = c.createStatement()) {
            s.execute("create or replace table t1 (c1 string, c2 string, c3 string, c4 string, c5 string, c6 string, c7 string, c8 string, c9 string)");

            String insertSQL = "insert into t1 values (?, ?, ?, ?, ?, ?, ?, ?, ?)";
            PreparedStatement ps = c.prepareStatement(insertSQL);

            ps.setNull(1, Types.NULL);
            ps.setNull(2, Types.NULL, "");
            ps.setString(3, null);
            ps.setDate(4, null);
            ps.setTimestamp(5, null);
            ps.setTime(6, null);
            ps.setObject(7, null);
            ps.setObject(8, null, Types.NULL);
            ps.setBinaryStream(9, null);
            // ps.setBlob(1, null); // can not be null

            if (batch) {
                ps.addBatch();
                int[] ans = ps.executeBatch();
                Assert.assertEquals(ans, new int[] {1});
            } else {
                int result = ps.executeUpdate();
                Assert.assertEquals(result, 1);
            }

            Statement statement = c.createStatement();
            statement.execute("SELECT * from t1");
            ResultSet r = statement.getResultSet();

            Assert.assertTrue(r.next());
            for (int i=1; i<=9; i++) {
                Assert.assertNull(r.getString(i));
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
                    "CREATE OR replace table %s(id TINYINT, obj VARIANT, s String, arr ARRAY(INT64)) Engine = Fuse"
                    , tableName);
            s.execute(createTableSQL);
            String insertSQL = String.format("insert into %s values %s", tableName, placeholder ? "(?,?,?,?)" : "");

            PreparedStatement ps = c.prepareStatement(insertSQL);
            ps.setInt(1, 1);
            ps.setString(2, "{\"a\": 1,\"b\": 2}");
            ps.setString(3, "hello world, 你好");
            ps.setString(4, "[1,2,3,4,5]");
            ps.addBatch();
            int[] ans = ps.executeBatch();
            Assert.assertEquals(ans.length, 1);
            Assert.assertEquals(ans[0], 1);

            s.execute(String.format("SELECT * from %s", tableName));
            ResultSet r = s.getResultSet();

            Assert.assertTrue(r.next());
            Assert.assertEquals(r.getInt(1), 1);
            Assert.assertEquals(r.getString(2), "{\"a\":1,\"b\":2}");
            Assert.assertEquals(r.getString(3), "hello world, 你好");
            Assert.assertEquals(r.getString(4), "[1,2,3,4,5]");

            Assert.assertFalse(r.next());
        }
    }
}
