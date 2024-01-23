package com.databend.jdbc;

import com.databend.client.PaginationOptions;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.sql.SQLException;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestDatabendDriverUri {
    private static DatabendDriverUri createDriverUri(String url)
            throws SQLException {
        Properties properties = new Properties();

        return DatabendDriverUri.create(url, properties);
    }

    private static void assertInvalid(String url, String prefix) {
        assertThatThrownBy(() -> createDriverUri(url))
                .isInstanceOf(SQLException.class)
                .hasMessageStartingWith(prefix);
    }

    @Test(groups = {"unit"})
    public void testInvalidUri() {
        // missing jdbc: prefix
        assertInvalid("test", "Invalid JDBC URL: test");

        // empty jdbc: url
        assertInvalid("jdbc:", "Invalid JDBC URL: jdbc:");
        assertInvalid("jdbc:databend://localhost:8080/default?SSL=", "Connection property 'ssl' value is empty");
        assertInvalid("jdbc:databend://localhost:8080?SSL=0", "Connection property 'ssl' value is invalid: 0");

    }

    @Test(groups = {"unit"})
    public void testBasic() throws SQLException {
        DatabendDriverUri uri = DatabendDriverUri.create("jdbc:databend://http://localhost", null);

        Assert.assertEquals(uri.getAddress().getHost(), "localhost");
        Assert.assertEquals(uri.getUri().getScheme(), "http");
        Assert.assertEquals(uri.getUri().getHost(), "localhost");
        Assert.assertEquals(uri.getUri().getPort(), 80);
    }

    @Test(groups = {"unit"})
    public void testDefaultSSL() throws SQLException {
        DatabendDriverUri uri = DatabendDriverUri.create("jdbc:databend://localhost", null);

        Assert.assertEquals(uri.getAddress().getHost(), "localhost");
        Assert.assertEquals(uri.getUri().getScheme(), "http");
        Assert.assertEquals(uri.getUri().getHost(), "localhost");
        Assert.assertEquals(uri.getUri().getPort(), 80);
    }

    @Test(groups = {"unit"})
    public void testSSLSetFalse() throws SQLException {
        DatabendDriverUri uri = DatabendDriverUri.create("jdbc:databend://localhost?SSL=false", null);

        Assert.assertEquals(uri.getAddress().getHost(), "localhost");
        Assert.assertEquals(uri.getUri().getScheme(), "http");
        Assert.assertEquals(uri.getUri().getHost(), "localhost");
        Assert.assertEquals(uri.getUri().getPort(), 80);
    }

    @Test(groups = {"unit"})
    public void testSSLSetTrue() throws SQLException {
        DatabendDriverUri uri = DatabendDriverUri.create("jdbc:databend://localhost?ssl=true", null);

        Assert.assertEquals(uri.getAddress().getHost(), "localhost");
        Assert.assertEquals(uri.getUri().getScheme(), "https");
        Assert.assertEquals(uri.getUri().getHost(), "localhost");
        Assert.assertEquals(uri.getUri().getPort(), 443);
    }

    @Test(groups = {"unit"})
    public void testSSLCustomPort() throws SQLException {
        DatabendDriverUri uri = DatabendDriverUri.create("jdbc:databend://localhost:33101", null);

        Assert.assertEquals(uri.getAddress().getHost(), "localhost");
        Assert.assertEquals(uri.getUri().getScheme(), "http");
        Assert.assertEquals(uri.getUri().getHost(), "localhost");
        Assert.assertEquals(uri.getUri().getPort(), 33101);
    }

    @Test(groups = {"unit"})
    public void testSSLCustomPort2() throws SQLException {
        DatabendDriverUri uri = DatabendDriverUri.create("jdbc:databend://http://localhost:33101", null);

        Assert.assertEquals(uri.getAddress().getHost(), "localhost");
        Assert.assertEquals(uri.getUri().getScheme(), "http");
        Assert.assertEquals(uri.getUri().getHost(), "localhost");
        Assert.assertEquals(uri.getUri().getPort(), 33101);

    }

    @Test(groups = {"unit"})
    public void testUser() throws SQLException {
        DatabendDriverUri uri = DatabendDriverUri.create("jdbc:databend://u1@localhost:33101?password=p1", null);

        Assert.assertEquals(uri.getProperties().get("user"), "u1");
        Assert.assertEquals(uri.getProperties().get("password"), "p1");
        Assert.assertEquals(uri.getDatabase(), "default");
    }

    @Test(groups = {"unit"})
    public void testUserDatabase() throws SQLException {
        DatabendDriverUri uri = DatabendDriverUri.create("jdbc:databend://u1@localhost:33101/db1?password=p1", null);

        Assert.assertEquals(uri.getProperties().get("user"), "u1");
        Assert.assertEquals(uri.getProperties().get("password"), "p1");
        Assert.assertEquals(uri.getDatabase(), "db1");
    }

    @Test(groups = {"unit"})
    public void testUserDatabasePath() throws SQLException {
        DatabendDriverUri uri = DatabendDriverUri.create("jdbc:databend://u1@localhost:33101/db1?password=p1&database=db2", null);

        Assert.assertEquals(uri.getProperties().get("user"), "u1");
        Assert.assertEquals(uri.getProperties().get("password"), "p1");
        Assert.assertEquals(uri.getDatabase(), "db2");
    }

    @Test(groups = {"unit"})
    public void testUserDatabaseProp() throws SQLException {
        Properties props = new Properties();
        props.setProperty("database", "db3");
        props.setProperty("SSL", "true");
        props.setProperty("binary_format", "base64");
        DatabendDriverUri uri = DatabendDriverUri.create("jdbc:databend://u1@localhost:33101/db1?password=p1&database=db2&query_timeout=120&connection_timeout=15&socket_timeout=15", props);

        Assert.assertEquals(uri.getProperties().get("user"), "u1");
        Assert.assertEquals(uri.getProperties().get("password"), "p1");
        Assert.assertEquals(uri.getDatabase(), "db3");
        Assert.assertEquals(uri.getUri().getScheme(), "https");
        Assert.assertEquals(uri.getUri().getHost(), "localhost");
        Assert.assertEquals(uri.getUri().getPort(), 33101);
        Assert.assertEquals(uri.getConnectionTimeout().intValue(), 15);
        Assert.assertEquals(uri.getQueryTimeout().intValue(), 120);
        Assert.assertEquals(uri.getSocketTimeout().intValue(), 15);
        Assert.assertEquals(uri.getWaitTimeSecs().intValue(), PaginationOptions.getDefaultWaitTimeSec());
        Assert.assertEquals(uri.getMaxRowsInBuffer().intValue(), PaginationOptions.getDefaultMaxRowsInBuffer());
        Assert.assertEquals(uri.getMaxRowsPerPage().intValue(), PaginationOptions.getDefaultMaxRowsPerPage());
        Assert.assertFalse(uri.presignedUrlDisabled().booleanValue());
        Assert.assertTrue(uri.copyPurge().booleanValue());
        Assert.assertEquals("\\N", uri.nullDisplay().toString());
        Assert.assertEquals("base64", uri.binaryFormat().toString());
    }

    @Test(groups = {"unit"})
    public void testUserDatabasePropFull() throws SQLException {
        Properties props = new Properties();
        props.setProperty("database", "db3");
        props.setProperty("SSL", "true");
        DatabendDriverUri uri = DatabendDriverUri.create("jdbc:databend://u1@localhost:33101/db1?password=p1&database=db2&query_timeout=120&connection_timeout=15&socket_timeout=15&presigned_url_disabled=true&wait_time_secs=1&max_rows_in_buffer=10&max_rows_per_page=5", props);

        Assert.assertEquals(uri.getProperties().get("user"), "u1");
        Assert.assertEquals(uri.getProperties().get("password"), "p1");
        Assert.assertEquals(uri.getDatabase(), "db3");
        Assert.assertEquals(uri.getUri().getScheme(), "https");
        Assert.assertEquals(uri.getUri().getHost(), "localhost");
        Assert.assertEquals(uri.getUri().getPort(), 33101);
        Assert.assertEquals(uri.getQueryTimeout().intValue(), 120);
        Assert.assertEquals(uri.getConnectionTimeout().intValue(), 15);
        Assert.assertEquals(uri.getSocketTimeout().intValue(), 15);
        Assert.assertEquals(uri.getWaitTimeSecs().intValue(), 1);
        Assert.assertEquals(uri.getMaxRowsInBuffer().intValue(), 10);
        Assert.assertEquals(uri.getMaxRowsPerPage().intValue(), 5);
        Assert.assertTrue(uri.presignedUrlDisabled().booleanValue());
        Assert.assertTrue(uri.copyPurge().booleanValue());
        Assert.assertEquals("hex", uri.binaryFormat().toString());
    }

    @Test(groups = {"unit"})
    public void testFull() throws SQLException {
        Properties props = new Properties();
        props.setProperty("database", "db3");
        props.setProperty("SSL", "true");
        props.setProperty("presigned_url_disabled", "false");
        props.setProperty("wait_time_secs", "9");
        props.setProperty("max_rows_in_buffer", "11");
        props.setProperty("max_rows_per_page", "7");
        props.setProperty("connection_time", "15");
        props.setProperty("warehouse", "test");
        props.setProperty("tenant", "tenant1");
        DatabendDriverUri uri = DatabendDriverUri.create("jdbc:databend://u1@localhost:33101/db1?password=p1&database=db2&tenant=tenant1&warehouse=test&null_display=null&connection_timeout=15&socket_timeout=15&presigned_url_disabled=true&wait_time_secs=1&max_rows_in_buffer=10&max_rows_per_page=5", props);

        Assert.assertEquals(uri.getProperties().get("user"), "u1");
        Assert.assertEquals(uri.getProperties().get("password"), "p1");
        Assert.assertEquals(uri.getProperties().get("warehouse"), "test");
        Assert.assertEquals(uri.getProperties().get("tenant"), "tenant1");
        Assert.assertEquals(uri.getDatabase(), "db3");
        Assert.assertEquals(uri.getUri().getScheme(), "https");
        Assert.assertEquals(uri.getUri().getHost(), "localhost");
        Assert.assertEquals(uri.getUri().getPort(), 33101);
        Assert.assertEquals(uri.getConnectionTimeout().intValue(), 15);
        Assert.assertEquals(uri.getSocketTimeout().intValue(), 15);
        Assert.assertEquals(uri.getWaitTimeSecs().intValue(), 9);
        Assert.assertEquals(uri.getMaxRowsInBuffer().intValue(), 11);
        Assert.assertEquals(uri.getMaxRowsPerPage().intValue(), 7);
        Assert.assertFalse(uri.presignedUrlDisabled().booleanValue());
        Assert.assertEquals("null", uri.nullDisplay().toString());
    }
}
