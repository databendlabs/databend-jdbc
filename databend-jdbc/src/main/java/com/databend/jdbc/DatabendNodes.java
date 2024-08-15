package com.databend.jdbc;

import java.net.URI;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

public class DatabendNodes implements DatabendNodeManager {

    private List<URI> query_nodes_uris;
    protected final AtomicInteger index;
    private final DatabendClientLoadBalancingPolicy policy;
    public DatabendNodes(List<URI> queryNodesUris, DatabendClientLoadBalancingPolicy policy) {
        this.query_nodes_uris = queryNodesUris;
        this.policy = policy;
        this.index = new AtomicInteger(0);
    }


    @Override
    public List<URI> getUris() {
        return query_nodes_uris;
    }

    public void updateNodes(List<URI> query_nodes_uris) {
        this.query_nodes_uris = query_nodes_uris;
    }

    @Override
    public DatabendClientLoadBalancingPolicy getPolicy() {
        return policy;
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
