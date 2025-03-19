package com.databend.jdbc;

import com.github.zafarkhaja.semver.Version;

import java.util.concurrent.atomic.AtomicLong;

public class QueryLiveness {
    String queryID;
    String nodeID;
    Version serverVersion;
    AtomicLong lastRequestTime;
    boolean stopped;
    long resultTimeoutSecs;
    public QueryLiveness(String queryID, String nodeID, AtomicLong lastRequestTime, Long resultTimeoutSecs, Version severVersion) {
        this.queryID = queryID;
        this.nodeID = nodeID;
        this.lastRequestTime = lastRequestTime;
        this.resultTimeoutSecs = resultTimeoutSecs;
        this.serverVersion = severVersion;
    }
}
