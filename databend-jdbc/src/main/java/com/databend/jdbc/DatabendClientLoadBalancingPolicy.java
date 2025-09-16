package com.databend.jdbc;

import java.net.URI;
import java.util.List;

class DatabendClientLoadBalancingPolicy {
    static class DisabledPolicy extends DatabendClientLoadBalancingPolicy {
        @Override
        public String toString() {
            return "Disabled";
        }
        // do nothing
    }

    static class RandomPolicy extends DatabendClientLoadBalancingPolicy {
        @Override
        protected URI pickUri(String query_id, DatabendNodes nodes) {
            List<URI> uris = nodes.getUris();

            if (uris == null || uris.isEmpty()) {
                return null;
            }

            // Generate a deterministic integer based on the query_id
            int deterministicValue = getQueryHash(query_id);

            // Use the deterministic value to select a URI
            int index = Math.abs(deterministicValue) % uris.size();

            return uris.get(index);
        }

        @Override
        public String toString() {
            return "Random";
        }
    }

    static class RoundRobinPolicy extends DatabendClientLoadBalancingPolicy {
        @Override
        protected URI pickUri(String query_id, DatabendNodes nodes) {
            List<URI> uris = nodes.getUris();

            if (uris == null || uris.isEmpty()) {
                return null;
            }

            // Use round robin to select a URI
            int index = nodes.index.getAndUpdate(v -> v + 1 >= uris.size() ? 0 : v + 1);

            return uris.get(index);
        }

        @Override
        public String toString() {
            return "RoundRobin";
        }
    }

    /**
     * Policy that disable load balance and always use the first node.
     */
    public static final String DISABLED = "disabled";
    /**
     * Policy to pick a node randomly from the list of available nodes.
     */
    public static final String RANDOM = "random";

    /**
     * Policy to pick a node using Round Robin Algorithm
     */
    public static final String ROUND_ROBIN = "round_robin";


    static DatabendClientLoadBalancingPolicy create(String name) {
        DatabendClientLoadBalancingPolicy policy;
        if (RANDOM.equalsIgnoreCase(name)) {
            policy = new RandomPolicy();
        } else if (ROUND_ROBIN.equalsIgnoreCase(name)) {
            policy = new RoundRobinPolicy();
        } else if (DISABLED.equalsIgnoreCase(name)) {
            policy = new DisabledPolicy();
        } else {
            throw new IllegalArgumentException("Unknown load balancing policy: " + name);
        }
        return policy;
    }


    /**
     * Policy to pick a node based on the least loaded algorithm.
     *
     * @param nodes the list of URIs to choose from
     * @return the URI to use
     */
    protected URI pickUri(String query_id, DatabendNodes nodes) {
        if (nodes == null || nodes.getUris() == null || nodes.getUris().isEmpty()) {
            return null;
        }
        return nodes.getUris().get(0);
    }


    /**
     * Get int hash value of given query id
     *
     * @param query_id the query id used for choosing load balancing node
     * @return hash value of the query id
     */
    private static int getQueryHash(String query_id) {
        if (query_id.isEmpty()) {
            return 0;
        }
        // Using the seed value
        int hash = 202011;
        for (char c : query_id.toCharArray()) {
            hash = hash * 31 + c;
        }
        return hash;
    }

}
