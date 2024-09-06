package com.databend.jdbc;

import com.databend.client.DiscoveryNode;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class TestMultiHost {
    private final String DEFAULT_JDBC_URL = "jdbc:databend://localhost:8000,localhost:8002,localhost:8003/default";
    private final String RANDOM_JDBC_URL = "jdbc:databend://localhost:8000,localhost:8002,localhost:8003/default?load_balancing_policy=random";
    private final String ROUND_ROBIN_JDBC_URL = "jdbc:databend://localhost:8000,localhost:8002,localhost:8003/default?load_balancing_policy=round_robin";
    private final String FAIL_OVER_JDBC_URL = "jdbc:databend://localhost:7222,localhost:7223,localhost:7224,localhost:8000/default?load_balancing_policy=round_robin&max_failover_retry=4";
    private final String AUTO_DISCOVERY_JDBC_URL = "jdbc:databend://localhost:8000/default?load_balancing_policy=round_robin&auto_discovery=true";
    private final String UNSUPPORT_AUTO_DISCOVERY_JDBC_URL = "jdbc:databend://localhost:8000/default?load_balancing_policy=round_robin&auto_discovery=true&enable_mock=true";


    private Connection createConnection(String url)
            throws SQLException {
        return DriverManager.getConnection(url, "databend", "databend");
    }

    @Test(groups = {"IT", "cluster"})
    public void testDefaultLoadBalancing()
            throws SQLException {
        // try connect with three nodes 1000 times and count for each node
        int node8000 = 0;
        int node8002 = 0;
        int node8003 = 0;
        int unknown = 0;
        for (int i = 0; i < 100; i++) {
            try (Connection connection = createConnection(DEFAULT_JDBC_URL)) {
                DatabendStatement statement = (DatabendStatement) connection.createStatement();
                statement.execute("select value from system.configs where name = 'http_handler_port';");
                ResultSet r = statement.getResultSet();
                r.next();
                if (r.getInt(1) == 8000) {
                    node8000++;
                } else if (r.getInt(1) == 8002) {
                    node8002++;
                } else if (r.getInt(1) == 8003) {
                    node8003++;
                } else {
                    unknown++;
                }
            }
        }
        Assert.assertEquals(node8000, 100);
        Assert.assertEquals(node8002, 0);
        Assert.assertEquals(node8003, 0);
        Assert.assertEquals(unknown, 0);
    }

    @Test(groups = {"IT", "cluster"})
    public void testRandomLoadBalancing()
            throws SQLException {
        // try connect with three nodes 1000 times and count for each node
        int node8000 = 0;
        int node8002 = 0;
        int node8003 = 0;
        int unknown = 0;
        for (int i = 0; i < 100; i++) {
            try (Connection connection = createConnection(RANDOM_JDBC_URL)) {
                DatabendStatement statement = (DatabendStatement) connection.createStatement();
                statement.execute("select value from system.configs where name = 'http_handler_port';");
                ResultSet r = statement.getResultSet();
                r.next();
                if (r.getInt(1) == 8000) {
                    node8000++;
                } else if (r.getInt(1) == 8002) {
                    node8002++;
                } else if (r.getInt(1) == 8003) {
                    node8003++;
                } else {
                    unknown++;
                }
            }
        }
        System.out.println("node8000: " + node8000 + ", node8002: " + node8002 + ", node8003: " + node8003 + ", unknown: " + unknown);
        Assert.assertTrue(node8000 > 0 && node8002 > 0 && node8003 > 0);
        Assert.assertEquals(unknown, 0);
        Assert.assertEquals(node8000 + node8002 + node8003, 100);
    }

    @Test(groups = {"IT", "cluster"})
    public void testRoundRobinLoadBalancing()
            throws SQLException {
        // try connect with three nodes 1000 times and count for each node
        int node8000 = 0;
        int node8002 = 0;
        int node8003 = 0;
        int unknown = 0;
        for (int i = 0; i < 30; i++) {
            try (Connection connection = createConnection(ROUND_ROBIN_JDBC_URL)) {
                DatabendStatement statement = (DatabendStatement) connection.createStatement();
                // remove the effect setup commands
                for (int j = 0; j < 3; j++) {
                    statement.execute("select value from system.configs where name = 'http_handler_port';");
                    ResultSet r = statement.getResultSet();
                    r.next();
                    if (r.getInt(1) == 8000) {
                        node8000++;
                    } else if (r.getInt(1) == 8002) {
                        node8002++;
                    } else if (r.getInt(1) == 8003) {
                        node8003++;
                    } else {
                        unknown++;
                    }
                }
            }
        }
        System.out.println("node8000: " + node8000 + ", node8002: " + node8002 + ", node8003: " + node8003 + ", unknown: " + unknown);
        Assert.assertEquals(node8000, 30);
        Assert.assertEquals(node8002, 30);
        Assert.assertEquals(node8003, 30);
        Assert.assertEquals(unknown, 0);
        Assert.assertEquals(node8000 + node8002 + node8003, 90);
    }

    @Test(groups = {"IT", "cluster"})
    public void testRoundRobinTransaction()
            throws SQLException {
        // try connect with three nodes 1000 times and count for each node
        try (Connection connection = createConnection(ROUND_ROBIN_JDBC_URL)) {
            DatabendStatement statement = (DatabendStatement) connection.createStatement();
            statement.execute("drop table if exists test_transaction;");
            statement.execute("create table if not exists test_transaction(id int);");

        }
        for (int i = 0; i < 30; i++) {
            try (Connection connection = createConnection(ROUND_ROBIN_JDBC_URL)) {
                DatabendStatement statement = (DatabendStatement) connection.createStatement();
                // use transaction select a table, drop a table, insert data into table bring i index
                statement.execute("begin;");
                statement.execute("insert into test_transaction values(" + i + ");");
                statement.execute("select * from test_transaction;");
                statement.execute("commit;");
            }
        }

        // query on test
        try (Connection connection = createConnection(ROUND_ROBIN_JDBC_URL)) {
            DatabendStatement statement = (DatabendStatement) connection.createStatement();
            statement.execute("select * from test_transaction;");
            ResultSet r = statement.getResultSet();
            int count = 0;
            while (r.next()) {
                count++;
            }
            Assert.assertEquals(count, 30);
        }
    }

    @Test(groups = {"IT", "cluster"})
    public void testFailOver()
            throws SQLException {
        // try connect with three nodes 1000 times and count for each node
        int node8000 = 0;
        int node8002 = 0;
        int node8003 = 0;
        int unknown = 0;
        for (int i = 0; i < 30; i++) {
            try (Connection connection = createConnection(FAIL_OVER_JDBC_URL)) {
                DatabendStatement statement = (DatabendStatement) connection.createStatement();
                // remove the effect setup commands
                for (int j = 0; j < 3; j++) {
                    statement.execute("select value from system.configs where name = 'http_handler_port';");
                    ResultSet r = statement.getResultSet();
                    r.next();
                    if (r.getInt(1) == 8000) {
                        node8000++;
                    } else if (r.getInt(1) == 8002) {
                        node8002++;
                    } else if (r.getInt(1) == 8003) {
                        node8003++;
                    } else {
                        unknown++;
                    }
                }
            }
        }
        System.out.println("node8000: " + node8000 + ", node8002: " + node8002 + ", node8003: " + node8003 + ", unknown: " + unknown);

        Assert.assertEquals(node8000, 90);
        Assert.assertEquals(unknown, 0);
        Assert.assertEquals(node8000 + node8002 + node8003, 90);
    }

    @Test(groups = {"IT", "cluster"})
    public void testAutoDiscovery()
            throws SQLException {
        // try connect with three nodes 1000 times and count for each node
        int node8000 = 0;
        int node8002 = 0;
        int node8003 = 0;
        int unknown = 0;
        try (Connection connection = createConnection(AUTO_DISCOVERY_JDBC_URL)) {
            for (int i = 0; i < 30; i++) {
                DatabendStatement statement = (DatabendStatement) connection.createStatement();
                // remove the effect setup commands
                for (int j = 0; j < 3; j++) {
                    statement.execute("select value from system.configs where name = 'http_handler_port';");
                    ResultSet r = statement.getResultSet();
                    r.next();
                    if (r.getInt(1) == 8000) {
                        node8000++;
                    } else if (r.getInt(1) == 8002) {
                        node8002++;
                    } else if (r.getInt(1) == 8003) {
                        node8003++;
                    } else {
                        unknown++;
                    }
                }
            }
        }
        System.out.println("node8000: " + node8000 + ", node8002: " + node8002 + ", node8003: " + node8003 + ", unknown: " + unknown);

        Assert.assertEquals(node8000, 31);
        Assert.assertEquals(node8002, 30);
        Assert.assertEquals(node8003, 29);
        Assert.assertEquals(unknown, 0);
        Assert.assertEquals(node8000 + node8002 + node8003, 90);
    }

    @Test(groups = {"IT", "cluster"})
    public void testUnSupportedAutoDiscovery()
            throws SQLException {
        try (Connection connection = createConnection(UNSUPPORT_AUTO_DISCOVERY_JDBC_URL)) {

            DatabendStatement statement = (DatabendStatement) connection.createStatement();
            statement.execute("select value from system.configs where name = 'http_handler_port';");
            ResultSet r = statement.getResultSet();
            r.next();
            DatabendConnection dbc = (DatabendConnection) connection;
            // automatically
            Assert.assertFalse(dbc.isAutoDiscovery());
        } catch (SQLException e) {
            // there should be no exception
            Assert.fail("Should not throw exception");
        }
    }

    @Test(groups = {"unit"})
    public void testAutoDiscoveryUriParsing() throws SQLException {
        DatabendDriverUri uri = DatabendDriverUri.create("jdbc:databend://localhost:8000?ssl=true", null);
        DatabendDriverUri uri2 = DatabendDriverUri.create("jdbc:databend://127.0.0.1:8000,127.0.0.1:8002,127.0.0.1:8003?ssl=true", null);
        List<URI> uris2 = uri2.getNodes().getUris();

        DatabendNodes nodes = uri.getNodes();
        List<DiscoveryNode> discoveryNodes = new ArrayList<>();
        discoveryNodes.add(DiscoveryNode.create("127.0.0.1:8000"));
        discoveryNodes.add(DiscoveryNode.create("127.0.0.1:8002"));
        discoveryNodes.add(DiscoveryNode.create("127.0.0.1:8003"));
        List<URI> uris = nodes.parseURI(discoveryNodes);
        for (URI u : uris) {
            System.out.println(u);
        }
        Assert.assertEquals(uris.size(), 3);
        Assert.assertEquals(uris2.size(), 3);
        Assert.assertEquals(uris2, uris);

    }
}
