package com.databend.jdbc;

import org.junit.Assert;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.Vector;

public class TestHeartbeat {
    @Test
    public void testHeartbeat() throws SQLException {
        Properties p = new Properties();
        p.setProperty("max_rows_in_buffer", "10000");
        // on the server side, the expired query is put into a queue before not deleted.
        // we need the query to expire before
        p.setProperty("max_threads", "32");
        p.setProperty("user", "databend");
        p.setProperty("password", "databend");
        try (Connection c1 = Utils.createConnection("default", p)) {
            Statement statement = c1.createStatement();
            statement.execute("set http_handler_result_timeout_secs=2");
            int n = 80000;
            int numQuery = 3;

            Vector<ResultSet> rss = new Vector();
            for (int i = 0; i < numQuery; i++) {
                statement = c1.createStatement();
                statement.executeQuery("select * from numbers(" + n + ") order by number");
                ResultSet rs = statement.getResultSet();
                rss.add(rs);
            }
            Thread.sleep(10000);

            // the asserts should fail on old version servers
            for (int i = 0; i < numQuery; i++) {
                ResultSet rs = rss.get(i);
                for (int j = 0; j < n; j++) {
                    Assert.assertEquals(true, rs.next());
                    Assert.assertEquals(j, rs.getInt(1));
                }
                Assert.assertEquals(false, rs.next());
                rs.close();
            }
            statement.close();
            Thread.sleep(5000);

            Assert.assertTrue(c1.unwrap(DatabendConnection.class).isHeartbeatStopped());

        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
