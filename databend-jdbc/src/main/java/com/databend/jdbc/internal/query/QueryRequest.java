package com.databend.jdbc.internal.query;

import com.databend.jdbc.internal.session.SessionState;
import com.databend.jdbc.internal.session.PaginationOptions;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

public class QueryRequest {
    private final String sql;
    private final String sessionId;
    private final PaginationOptions paginationOptions;
    private final SessionState session;
    private final StageAttachment stageAttachment;
    private final Integer arrowResultVersionMax;

    @JsonCreator
    public QueryRequest(
            @JsonProperty("sql") String sql,
            @JsonProperty("session_id") String sessionId,
            @JsonProperty("pagination") PaginationOptions paginationOptions,
            @JsonProperty("session") SessionState session,
            @JsonProperty("stage_attachment") StageAttachment stageAttachment,
            @JsonProperty("arrow_result_version_max") Integer arrowResultVersionMax) {
        this.sql = sql;
        this.sessionId = sessionId;
        this.paginationOptions = paginationOptions;
        this.session = session;
        this.stageAttachment = stageAttachment;
        this.arrowResultVersionMax = arrowResultVersionMax;
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
    public SessionState getSession() {
        return session;
    }

    @JsonProperty("stage_attachment")
    public StageAttachment getStageAttachment() {
        return stageAttachment;
    }

    @JsonProperty("arrow_result_version_max")
    public Integer getArrowResultVersionMax() {
        return arrowResultVersionMax;
    }

    @Override
    public String toString() {
        try {
            return new ObjectMapper().writeValueAsString(this);
        } catch (Exception e) {
            return null;
        }
    }

    public static class Builder {
        private String sql;
        private String sessionId;
        private PaginationOptions paginationOptions;
        private SessionState session;
        private StageAttachment stageAttachment;
        private Integer arrowResultVersionMax;

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

        public Builder setSession(SessionState session) {
            this.session = session;
            return this;
        }

        public Builder setStageAttachment(StageAttachment stageAttachment) {
            this.stageAttachment = stageAttachment;
            return this;
        }

        public Builder setArrowResultVersionMax(Integer arrowResultVersionMax) {
            this.arrowResultVersionMax = arrowResultVersionMax;
            return this;
        }

        public QueryRequest build() {
            return new QueryRequest(sql, sessionId, paginationOptions, session, stageAttachment, arrowResultVersionMax);
        }
    }
}
