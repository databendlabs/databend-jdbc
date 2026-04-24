package com.databend.jdbc.internal.session;

import java.util.concurrent.atomic.AtomicLong;

public class QueryLiveness {
    public final String queryID;
    public final String nodeID;
    public final boolean serverSupportHeartBeat;
    public final AtomicLong lastRequestTime;
    public volatile boolean stopped;
    public final long resultTimeoutSecs;

    public QueryLiveness(
            String queryID,
            String nodeID,
            AtomicLong lastRequestTime,
            Long resultTimeoutSecs,
            boolean serverSupportHeartBeat) {
        this.queryID = queryID;
        this.nodeID = nodeID;
        this.lastRequestTime = lastRequestTime;
        this.resultTimeoutSecs = resultTimeoutSecs;
        this.serverSupportHeartBeat = serverSupportHeartBeat;
    }
}
