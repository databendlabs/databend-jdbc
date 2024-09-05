package com.databend.jdbc;

import com.databend.client.ClientSettings;
import com.databend.client.DatabendClientV1;
import com.databend.client.DiscoveryNode;
import okhttp3.OkHttpClient;

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.databend.jdbc.ConnectionProperties.SSL;
import static com.databend.jdbc.ConnectionProperties.SSL_MODE;

public class DatabendNodes implements DatabendNodeRouter {

    private AtomicReference<List<URI>> query_nodes_uris;
    protected final AtomicInteger index;
    // keep track of latest discovery scheduled time
    protected final AtomicReference<Long> lastDiscoveryTime = new AtomicReference<>(0L);
    // minimum time between discovery
    protected long discoveryInterval = 1000 * 60 * 5;
    protected DatabendClientLoadBalancingPolicy policy;

    private final String uriPath;
    private final String uriQuery;
    private final String uriFragment;

    private boolean useSecureConnection = false;
    private String sslmode = "disable";

    public DatabendNodes(List<URI> queryNodesUris, DatabendClientLoadBalancingPolicy policy, String UriPath, String UriQuery, String UriFragment) {
        this.query_nodes_uris = new AtomicReference<>(queryNodesUris);
        this.policy = policy;
        this.index = new AtomicInteger(0);
        this.uriPath = UriPath;
        this.uriQuery = UriQuery;
        this.uriFragment = UriFragment;
    }

    @Override
    public List<URI> getUris() {
        return query_nodes_uris.get();
    }

    public void setSSL(boolean useSecureConnection, String sslmode) {
        this.useSecureConnection = useSecureConnection;
        this.sslmode = sslmode;
    }

    public void updateNodes(List<URI> query_nodes_uris) {
        this.query_nodes_uris.set(query_nodes_uris);
    }

    public void updatePolicy(DatabendClientLoadBalancingPolicy policy) {
        this.policy = policy;
    }

    @Override
    public DatabendClientLoadBalancingPolicy getPolicy() {
        return policy;
    }

    @Override
    public boolean discoverUris(OkHttpClient client, ClientSettings settings) {
        // do nothing if discovery interval is not reached
        Long lastDiscoveryTime = this.lastDiscoveryTime.get();
        if (System.currentTimeMillis() - lastDiscoveryTime < discoveryInterval) {
            return false;
        }
        List<URI> current_nodes = query_nodes_uris.get();
        if (!this.lastDiscoveryTime.compareAndSet(lastDiscoveryTime, System.currentTimeMillis())) {
            return false;
        }

        List<DiscoveryNode> new_nodes = DatabendClientV1.dicoverNodes(client, settings);
        if (!new_nodes.isEmpty()) {
            // convert new nodes using lambda
            List<URI> new_uris = new_nodes.stream().map(node -> URI.create("http://" + node.getAddress())).collect(Collectors.toList());
            updateNodes(new_uris);
            return true;
        }
        return false;
    }

    private List<URI> parseURI(List<DiscoveryNode> nodes) throws SQLException {
        String host = null;
        List<URI> uris = new ArrayList<>();
        try {
            for (DiscoveryNode node : nodes) {
                String raw_host = node.getAddress();
                String fullUri = (raw_host.startsWith("http://") || raw_host.startsWith("https://")) ?
                        raw_host  :
                        "http://" + raw_host;

                URI uri = new URI(fullUri);
                String authority = uri.getAuthority();
                String[] hostAndPort = authority.split(":");
                if (hostAndPort.length == 2) {
                    host = hostAndPort[0];
                } else if (hostAndPort.length == 1) {
                    host = hostAndPort[0];
                } else {
                    throw new SQLException("Invalid host and port, url: " + uri);
                }
                if (host == null || host.isEmpty()) {
                    throw new SQLException("Invalid host " + host);
                }

                uris.add(new URI(uri.getScheme(), uri.getUserInfo(), uri.getHost(), uri.getPort(), uriPath, uriQuery, uriFragment));
            }

        } catch (URISyntaxException e) {
            throw new SQLException("Invalid URI", e.getMessage());
        }

        return uris;
    }
    public URI pickUri(String query_id) {
        return policy.pickUri(query_id, this);
    }
    @Override
    public String toString() {
        return "DatabendNodes{" +
                "query_nodes_uris=" + query_nodes_uris +
                ", policy=" + policy +
                '}';
    }
}
