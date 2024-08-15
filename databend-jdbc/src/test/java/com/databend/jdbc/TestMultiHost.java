package com.databend.jdbc;

import com.databend.client.PaginationOptions;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

public class TestMultiHost {
    private final  String DEFAULT_JDBC_URL = "jdbc:databend://localhost:8000,localhost:8001,localhost:8002/default";
    private Connection createConnection(String url)
            throws SQLException {
        return DriverManager.getConnection(url, "databend", "databend");
    }

    @Test(groups = {"IT"})
    public void testRandomLoadBalancing()
            throws SQLException {
        try (Connection connection = createConnection(DEFAULT_JDBC_URL)) {
            PaginationOptions p = connection.unwrap(DatabendConnection.class).getPaginationOptions();
            Assert.assertEquals(p.getWaitTimeSecs(), PaginationOptions.getDefaultWaitTimeSec());
            Assert.assertEquals(p.getMaxRowsInBuffer(), PaginationOptions.getDefaultMaxRowsInBuffer());
            Assert.assertEquals(p.getMaxRowsPerPage(), PaginationOptions.getDefaultMaxRowsPerPage());
            DatabendStatement statement = (DatabendStatement) connection.createStatement();
            statement.execute("SELECT number from numbers(200000) order by number");
            ResultSet r = statement.getResultSet();
            r.next();
            for (int i = 1; i < 1000; i++) {
                r.next();
                Assert.assertEquals(r.getInt(1), i);
            }
            connection.close();
        } finally {

        }
    }
}
