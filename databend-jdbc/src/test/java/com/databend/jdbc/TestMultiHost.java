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
import java.util.HashMap;
import java.util.List;

@Test(timeOut = 10000, groups = "MULTI_HOST" )
public class TestMultiHost {
    private final String DEFAULT_JDBC_URL = "jdbc:databend://localhost:8001,localhost:8002,localhost:8003/default";
    private final String RANDOM_JDBC_URL = "jdbc:databend://localhost:8001,localhost:8002,localhost:8003/default?load_balancing_policy=random";
    private final String ROUND_ROBIN_JDBC_URL = "jdbc:databend://localhost:8001,localhost:8002,localhost:8003/default?load_balancing_policy=round_robin";
    private final String FAIL_OVER_JDBC_URL = "jdbc:databend://localhost:7222,localhost:7223,localhost:7224,localhost:8001/default?load_balancing_policy=round_robin&max_failover_retry=4";
    private final String AUTO_DISCOVERY_JDBC_URL = "jdbc:databend://localhost:8001/default?load_balancing_policy=round_robin&auto_discovery=true";
    private final String UNSUPPORT_AUTO_DISCOVERY_JDBC_URL = "jdbc:databend://localhost:8001/default?load_balancing_policy=round_robin&auto_discovery=true&enable_mock=true";


    private Connection createConnection(String url)
            throws SQLException {
        return DriverManager.getConnection(url, "databend", "databend");
    }

    @Test(groups = {"IT", "MULTI_HOST"})
    public void testDefaultLoadBalancing()
            throws SQLException {
        HashMap<Integer, Integer> expect = new HashMap<>();
        expect.put(8001, 90);

        HashMap<Integer, Integer> actual = get_hosts_used(DEFAULT_JDBC_URL);
        Assert.assertEquals(expect, actual);
    }

    @Test(groups = {"IT", "MULTI_HOST"})
    public void testRandomLoadBalancing()
            throws SQLException {
        HashMap<Integer, Integer> actual = get_hosts_used(RANDOM_JDBC_URL);

        int node8001 = actual.get(8001);
        int node8002 = actual.get(8002);
        int node8003 = actual.get(8003);

        Assert.assertTrue(node8001 > 0 && node8002 > 0 && node8003 > 0, "got " + actual);
        Assert.assertEquals(node8001 + node8002 + node8003, 90, "got " + actual);
    }

    @Test(groups = {"IT", "MULTI_HOST"})
    public void testRoundRobinLoadBalancing()
            throws SQLException {
        HashMap<Integer, Integer> expect = new HashMap<>();
        expect.put(8001, 30);
        expect.put(8002, 30);
        expect.put(8003, 30);

        HashMap<Integer, Integer> actual = get_hosts_used(ROUND_ROBIN_JDBC_URL);
        Assert.assertEquals(expect, actual);
    }

    @Test(groups = {"IT", "MULTI_HOST"})
    public void testRoundRobinTransaction()
            throws SQLException {
        // try to connect with three nodes 1000 times and count for each node
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
    // @Test(groups = {"IT", "MULTI_HOST"})
    // skip since getConnection not support multihost for now
//    public void testFailOver()
//            throws SQLException {
//        HashMap<Integer, Integer> expect = new HashMap<>();
//        expect.put(8001, 90);
//
//        HashMap<Integer, Integer> actual = get_hosts_used(FAIL_OVER_JDBC_URL);
//        Assert.assertEquals(expect, actual);
//    }

    @Test(groups = {"IT", "MULTI_HOST"})
    public void testAutoDiscovery()
            throws SQLException {
        HashMap<Integer, Integer> expect = new HashMap<>();
        expect.put(8001, 31);
        expect.put(8002, 30);
        expect.put(8003, 29);

        HashMap<Integer, Integer> actual = get_hosts_used(AUTO_DISCOVERY_JDBC_URL);
        Assert.assertEquals(expect, actual);
    }

    @Test(groups = {"IT", "MULTI_HOST"})
    public void testUnSupportedAutoDiscovery()
            throws Exception {
        try (Connection connection = createConnection(UNSUPPORT_AUTO_DISCOVERY_JDBC_URL)) {
            DatabendStatement statement = (DatabendStatement) connection.createStatement();
            statement.execute("select value from system.configs where name = 'http_handler_port';");
            ResultSet r = statement.getResultSet();
            r.next();

            Assert.assertFalse((boolean) Compatibility.invokeMethodNoArg(connection, "isAutoDiscovery"));
        }
    }

    @Test(groups = {"UNIT"})
    public void testAutoDiscoveryUriParsing() throws SQLException {
        DatabendDriverUri uri = DatabendDriverUri.create("jdbc:databend://localhost:8001?ssl=true", null);
        DatabendDriverUri uri2 = DatabendDriverUri.create("jdbc:databend://127.0.0.1:8001,127.0.0.1:8002,127.0.0.1:8003?ssl=true", null);
        List<URI> uris2 = uri2.getNodes().getUris();

        DatabendNodes nodes = uri.getNodes();
        List<DiscoveryNode> discoveryNodes = new ArrayList<>();
        discoveryNodes.add(DiscoveryNode.create("127.0.0.1:8001"));
        discoveryNodes.add(DiscoveryNode.create("127.0.0.1:8002"));
        discoveryNodes.add(DiscoveryNode.create("127.0.0.1:8003"));
        List<URI> uris = nodes.parseURI(discoveryNodes);
        Assert.assertEquals(uris.size(), 3);
        Assert.assertEquals(uris2.size(), 3);
        Assert.assertEquals(uris2, uris);
    }

    private HashMap<Integer, Integer> get_hosts_used(String dsn) throws SQLException {
        HashMap<Integer, Integer> ports = new HashMap<>();
        try (Connection connection = createConnection(dsn)) {
            for (int i = 0; i < 30; i++) {
                DatabendStatement statement = (DatabendStatement) connection.createStatement();
                // remove the effect setup commands
                for (int j = 0; j < 3; j++) {
                    statement.execute("select value from system.configs where name = 'http_handler_port';");
                    ResultSet r = statement.getResultSet();
                    r.next();
                    int p = r.getInt(1);
                    ports.merge(p, 1, Integer::sum);
                }
            }
        }
        return ports;
    }
}
