package com.databend.jdbc;

import com.databend.client.ClientSettings;
import okhttp3.OkHttpClient;

import java.net.URI;
import java.util.List;

/**
 *  Node manager manage a list of hosts
 */
public interface DatabendNodeRouter {
    /**
     * Gets a copy of all possible query uris
     *
     * @return non-null uris
     */
    List<URI> getUris();

    /**
     * Get load balancing policy
     */
    DatabendClientLoadBalancingPolicy getPolicy();

    /**
     * Discover all possible query uris through databend discovery api and update candidate node router list in need
     * @return true if update operation executed, false otherwise
     * Ref PR:
     * https://github.com/datafuselabs/databend-jdbc/pull/264
     * https://github.com/datafuselabs/databend/pull/16353
     */
    boolean discoverUris(OkHttpClient client, ClientSettings settings);
}
