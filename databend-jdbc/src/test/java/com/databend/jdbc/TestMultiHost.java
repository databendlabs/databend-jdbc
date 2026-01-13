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
    private final String AUTO_DISCOVERY_JDBC_URL = "jdbc:databend://localhost:8001/default?load_balancing_policy=round_robin&auto_discovery=true";
    private final String UNSUPPORT_AUTO_DISCOVERY_JDBC_URL = "jdbc:databend://localhost:8001/default?load_balancing_policy=round_robin&auto_discovery=true&enable_mock=true";


    private Connection createConnection(String url)
            throws SQLException {
        return DriverManager.getConnection(url, "databend", "databend");
    }

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
        DatabendNodes nodes = uri.getNodes();
        List<DiscoveryNode> discoveryNodes = new ArrayList<>();
        discoveryNodes.add(DiscoveryNode.create("127.0.0.1:8001"));
        discoveryNodes.add(DiscoveryNode.create("127.0.0.1:8002"));
        discoveryNodes.add(DiscoveryNode.create("127.0.0.1:8003"));
        List<URI> uris = nodes.parseURI(discoveryNodes);
        Assert.assertEquals(uris.size(), 3);
        Assert.assertEquals(uris.get(0).getScheme(), "https");
        Assert.assertEquals(uris.get(0).getHost(), "127.0.0.1");
        Assert.assertEquals(uris.get(0).getPath(), "");
        Assert.assertEquals(uris.get(0).getQuery(), "ssl=true");
        Assert.assertEquals(uris.get(0).getPort(), 8001);
        Assert.assertEquals(uris.get(1).getPort(), 8002);
        Assert.assertEquals(uris.get(2).getPort(), 8003);
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
