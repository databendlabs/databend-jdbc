package com.databend.jdbc;

import java.util.concurrent.atomic.AtomicLong;

public class QueryLiveness {
    String queryID;
    String nodeID;
    boolean serverSupportHeartBeat;
    AtomicLong lastRequestTime;
    boolean stopped;
    long resultTimeoutSecs;
    public QueryLiveness(String queryID, String nodeID, AtomicLong lastRequestTime, Long resultTimeoutSecs, boolean serverSupportHeartBeat) {
        this.queryID = queryID;
        this.nodeID = nodeID;
        this.lastRequestTime = lastRequestTime;
        this.resultTimeoutSecs = resultTimeoutSecs;
        this.serverSupportHeartBeat = serverSupportHeartBeat;
    }
}
