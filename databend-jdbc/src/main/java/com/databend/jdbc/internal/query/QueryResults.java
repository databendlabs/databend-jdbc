package com.databend.jdbc.internal.query;

import com.databend.jdbc.internal.error.QueryError;
import com.databend.jdbc.internal.data.ParseJsonDataUtils;
import com.databend.jdbc.internal.session.SessionState;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.net.URI;
import java.util.List;
import java.util.Map;

import static com.google.common.base.MoreObjects.toStringHelper;

public class QueryResults {
    private final String queryId;
    private final String nodeId;
    private final String sessionId;
    private final SessionState session;
    private final List<QueryRowField> schema;
    private final List<List<String>> data;
    private final Map<String, String> settings;
    private final String state;
    private final QueryError error;
    private final QueryStats stats;
    private final QueryAffect affect;
    private final long resultTimeoutSecs;
    private final URI statsUri;
    private final URI finalUri;
    private final URI nextUri;
    private final URI killUri;

    @JsonCreator
    public QueryResults(
            @JsonProperty("id") String queryId,
            @JsonProperty("node_id") String nodeId,
            @JsonProperty("session_id") String sessionId,
            @JsonProperty("session") SessionState session,
            @JsonProperty("schema") List<QueryRowField> schema,
            @JsonProperty("data") List<List<String>> data,
            @JsonProperty("settings") Map<String, String> settings,
            @JsonProperty("state") String state,
            @JsonProperty("error") QueryError error,
            @JsonProperty("stats") QueryStats stats,
            @JsonProperty("affect") QueryAffect affect,
            @JsonProperty("result_timeout_secs") long resultTimeoutSecs,
            @JsonProperty("stats_uri") URI statsUri,
            @JsonProperty("final_uri") URI finalUri,
            @JsonProperty("next_uri") URI nextUri,
            @JsonProperty("kill_uri") URI killUri) {
        this.queryId = queryId;
        this.nodeId = nodeId;
        this.sessionId = sessionId;
        this.session = session;
        this.schema = schema;
        this.data = data;
        this.settings = settings;
        this.state = state;
        this.error = error;
        this.stats = stats;
        this.affect = affect;
        this.resultTimeoutSecs = resultTimeoutSecs;
        this.statsUri = statsUri;
        this.finalUri = finalUri;
        this.nextUri = nextUri;
        this.killUri = killUri;
    }

    @JsonProperty("id")
    public String getQueryId() {
        return queryId;
    }

    @JsonProperty("node_id")
    public String getNodeId() {
        return nodeId;
    }

    @JsonProperty("session_id")
    public String getSessionId() {
        return sessionId;
    }

    @JsonProperty("session")
    public SessionState getSession() {
        return session;
    }

    @JsonProperty("schema")
    public List<QueryRowField> getSchema() {
        return schema;
    }

    @JsonProperty
    public List<List<Object>> getData() {
        return ParseJsonDataUtils.parseRawData(schema, data);
    }

    @JsonProperty
    public List<List<String>> getDataRaw() {
        return data;
    }

    public Map<String, String> getSettings() {
        return settings;
    }

    @JsonProperty
    public String getState() {
        return state;
    }

    @JsonProperty
    public QueryError getError() {
        return error;
    }

    @JsonProperty
    public QueryStats getStats() {
        return stats;
    }

    @JsonProperty
    public QueryAffect getAffect() {
        return affect;
    }

    @JsonProperty("stats_uri")
    public URI getStatsUri() {
        return statsUri;
    }

    @JsonProperty("final_uri")
    public URI getFinalUri() {
        return finalUri;
    }

    @JsonProperty("next_uri")
    public URI getNextUri() {
        return nextUri;
    }

    @JsonProperty("kill_uri")
    public URI getKillUri() {
        return killUri;
    }

    @JsonProperty("result_timeout_secs")
    public Long getResultTimeoutSecs() {
        return resultTimeoutSecs;
    }

    public boolean hasMoreData() {
        return nextUri != null && !nextUri.getPath().contains("/final");
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("id", queryId)
                .add("sessionId", sessionId)
                .add("session", session)
                .add("schema", schema)
                .add("data", data)
                .add("settings", settings)
                .add("state", state)
                .add("error", error)
                .add("stats", stats)
                .add("affect", affect)
                .add("statsUri", statsUri)
                .add("finalUri", finalUri)
                .add("nextUri", nextUri)
                .add("killUri", killUri)
                .toString();
    }
}
