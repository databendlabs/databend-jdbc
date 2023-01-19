/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.databend.client;

import com.databend.client.errors.QueryErrors;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.net.URI;
import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;

public class QueryResults {
    private final String id;
    private final String sessionId;
    private final DatabendSession session;
    private final List<QueryRowField> schema;
    private final Iterable<List<Object>> data;
    private final String state;
    private final QueryErrors error;
    private final QueryStats stats;
    private final QueryAffect affect;
    private final URI statsUri;

    private final URI finalUri;
    private final URI nextUri;
    private final URI killUri;

    @JsonCreator
    public QueryResults(
            @JsonProperty("id") String id,
            @JsonProperty("session_id") String sessionId,
            @JsonProperty("session") DatabendSession session,
            @JsonProperty("schema") List<QueryRowField> schema,
            @JsonProperty("data") List<List<Object>> data,
            @JsonProperty("state") String state,
            @JsonProperty("error") QueryErrors error,
            @JsonProperty("stats") QueryStats stats,
            @JsonProperty("affect") QueryAffect affect,
            @JsonProperty("stats_uri") URI statsUri,
            @JsonProperty("final_uri") URI finalUri,
            @JsonProperty("next_uri") URI nextUri,
            @JsonProperty("kill_uri") URI killUri) {
        this.id = id;
        this.sessionId = sessionId;
        this.session = session;
        this.schema = schema;
        this.data = ParseJsonDataUtils.parseRawData(schema, data);
        this.state = state;
        this.error = error;
        this.stats = stats;
        this.affect = affect;
        this.statsUri = statsUri;
        this.finalUri = finalUri;
        this.nextUri = nextUri;
        this.killUri = killUri;
    }

    // add builder

    @JsonProperty
    public String getId() {
        return id;
    }

    @JsonProperty
    public String getSessionId() {
        return sessionId;
    }

    @JsonProperty
    public DatabendSession getSession() {
        return session;
    }

    @JsonProperty
    public List<QueryRowField> getSchema() {
        return schema;
    }

    @JsonProperty
    public Iterable<List<Object>> getData() {
        return data;
    }

    @JsonProperty
    public String getState() {
        return state;
    }

    @JsonProperty
    public QueryErrors getError() {
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

    @JsonProperty
    public URI getStatsUri() {
        return statsUri;
    }

    @JsonProperty
    public URI getFinalUri() {
        return finalUri;
    }

    @JsonProperty
    public URI getNextUri() {
        return nextUri;
    }

    @JsonProperty
    public URI getKillUri() {
        return killUri;
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("id", id)
                .add("sessionId", sessionId)
                .add("session", session)
                .add("schema", schema)
                .add("data", data)
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
