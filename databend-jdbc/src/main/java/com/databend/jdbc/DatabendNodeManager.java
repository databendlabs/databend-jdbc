package com.databend.jdbc;

import java.net.URI;
import java.util.List;

/**
 *  Node manager manage a list of hosts
 */
public interface DatabendNodeManager {
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

}
