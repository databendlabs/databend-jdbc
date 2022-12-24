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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

public class QueryRequest {
    private final String sql;

    // should be careful about session id since it is hard to track on the in memory state,
    // prefer client-side DatabendSession settings for stateless session management
    private final String sessionId;
    private final PaginationOptions paginationOptions;
    private final DatabendSession session;

    private final StageAttachment stageAttachment;

    @JsonCreator
    public QueryRequest(
            @JsonProperty("sql") String sql,
            @JsonProperty("session_id") String sessionId,
            @JsonProperty("pagination") PaginationOptions paginationOptions,
            @JsonProperty("session") DatabendSession session,
            @JsonProperty("stage_attachment") StageAttachment stageAttachment
    ) {
        this.sql = sql;
        this.sessionId = sessionId;
        this.paginationOptions = paginationOptions;
        this.session = session;
        this.stageAttachment = stageAttachment;
    }

    public static Builder builder() {
        return new Builder();
    }

    @JsonProperty("sql")
    public String getSql() {
        return sql;
    }

    @JsonProperty("session_id")
    public String getSessionId() {
        return sessionId;
    }

    @JsonProperty("pagination")
    public PaginationOptions getPaginationOptions() {
        return paginationOptions;
    }

    @JsonProperty("session")
    public DatabendSession getSession() {
        return session;
    }

    @JsonProperty("stage_attachment")
    public StageAttachment getStageAttachment() {
        return stageAttachment;
    }

    @Override
    public String toString() {
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.writeValueAsString(this);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public static class Builder {
        private String sql;
        private String sessionId;
        private PaginationOptions paginationOptions;
        private DatabendSession session;
        private StageAttachment stageAttachment;

        public Builder setSql(String sql) {
            this.sql = sql;
            return this;
        }

        public Builder setSessionId(String sessionId) {
            this.sessionId = sessionId;
            return this;
        }

        public Builder setPaginationOptions(PaginationOptions paginationOptions) {
            this.paginationOptions = paginationOptions;
            return this;
        }

        public Builder setSession(DatabendSession session) {
            this.session = session;
            return this;
        }

        public Builder setStageAttachment(StageAttachment stageAttachment) {
            this.stageAttachment = stageAttachment;
            return this;
        }

        public QueryRequest build() {
            return new QueryRequest(sql, sessionId, paginationOptions, session, stageAttachment);
        }
    }
}
