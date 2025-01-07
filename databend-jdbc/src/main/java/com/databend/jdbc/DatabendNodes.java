package com.databend.jdbc;

import com.databend.client.ClientSettings;
import com.databend.client.DatabendClientV1;
import com.databend.client.DiscoveryNode;
import lombok.Setter;
import okhttp3.OkHttpClient;

import java.net.URI;
import java.net.URISyntaxException;
import java.security.InvalidParameterException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

public class DatabendNodes implements DatabendNodeRouter {

    private AtomicReference<List<URI>> query_nodes_uris;
    protected final AtomicInteger index;
    // keep track of latest discovery scheduled time
    protected final AtomicReference<Long> lastDiscoveryTime = new AtomicReference<>(0L);
    private static final Logger logger = Logger.getLogger(DatabendNodes.class.getPackage().getName());
    @Setter
    private boolean debug = false;
    // minimum time between discovery
    protected long discoveryInterval;
    protected DatabendClientLoadBalancingPolicy policy;

    private final String uriPath;
    private final String uriQuery;
    private final String uriFragment;

    private boolean useSecureConnection = false;
    private String sslmode = "disable";

    public DatabendNodes(List<URI> queryNodesUris, DatabendClientLoadBalancingPolicy policy, String UriPath, String UriQuery, String UriFragment, long discoveryInterval) {
        this.query_nodes_uris = new AtomicReference<>(queryNodesUris);
        this.policy = policy;
        this.index = new AtomicInteger(0);
        this.uriPath = UriPath;
        this.uriQuery = UriQuery;
        this.uriFragment = UriFragment;
        this.discoveryInterval = discoveryInterval;
    }

    @Override
    public List<URI> getUris() {
        return query_nodes_uris.get();
    }

    public void setSSL(boolean useSecureConnection, String sslmode) {
        this.useSecureConnection = useSecureConnection;
        this.sslmode = sslmode;
    }

    public void setDiscoveryInterval(long discoveryInterval) {
        this.discoveryInterval = discoveryInterval;
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
    public void discoverUris(OkHttpClient client, ClientSettings settings) throws UnsupportedOperationException {
        // do nothing if discovery interval is not reached
        Long lastDiscoveryTime = this.lastDiscoveryTime.get();
        if (System.currentTimeMillis() - lastDiscoveryTime < discoveryInterval) {
            return;
        }
        List<URI> current_uris = query_nodes_uris.get();
        if (!this.lastDiscoveryTime.compareAndSet(lastDiscoveryTime, System.currentTimeMillis())) {
            return;
        }
        try {
            List<DiscoveryNode> new_nodes = DatabendClientV1.discoverNodes(client, settings);
            if (!new_nodes.isEmpty()) {
                // convert new nodes using lambda
                List<URI> new_uris = this.parseURI(new_nodes);
                if (this.query_nodes_uris.compareAndSet(current_uris, new_uris)) {
                    java.util.logging.Level level = debug ? java.util.logging.Level.INFO : java.util.logging.Level.FINE;
                    // the log would only show that when truly updated the nodes
                    logger.log(level, "Automatic Discovery updated nodes: " + new_uris);
                }
            }
        } catch (UnsupportedOperationException e) {
            throw e;
        } catch (RuntimeException e) {
            logger.log(java.util.logging.Level.WARNING, "Error updating nodes: " + e.getMessage());
        }

    }

    @Override
    public boolean needDiscovery() {
        Long lastDiscoveryTime = this.lastDiscoveryTime.get();
        return System.currentTimeMillis() - lastDiscoveryTime >= discoveryInterval;
    }

    public List<URI> parseURI(List<com.databend.client.DiscoveryNode> nodes) throws RuntimeException {
        String host = null;
        List<URI> uris = new ArrayList<>();
        try {
            for (DiscoveryNode node : nodes) {
                String raw_host = node.getAddress();
                String fullUri = (raw_host.startsWith("http://") || raw_host.startsWith("https://")) ?
                        raw_host :
                        "http://" + raw_host;

                URI uri = new URI(fullUri);
                String authority = uri.getAuthority();
                String[] hostAndPort = authority.split(":");
                if (hostAndPort.length == 2) {
                    host = hostAndPort[0];
                } else if (hostAndPort.length == 1) {
                    host = hostAndPort[0];
                } else {
                    throw new InvalidParameterException("Invalid host and port, url: " + uri);
                }
                if (host == null || host.isEmpty()) {
                    throw new InvalidParameterException("Invalid host " + host);
                }

                uris.add(new URI(uri.getScheme(), uri.getUserInfo(), uri.getHost(), uri.getPort(), uriPath, uriQuery, uriFragment));
            }
            return DatabendDriverUri.canonicalizeUris(uris, this.useSecureConnection, this.sslmode);
        } catch (URISyntaxException e) {
            throw new InvalidParameterException("Invalid URI " + e.getMessage());
        } catch (SQLException e) {
            throw new RuntimeException("Error parsing URI " + e.getMessage());
        }
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
