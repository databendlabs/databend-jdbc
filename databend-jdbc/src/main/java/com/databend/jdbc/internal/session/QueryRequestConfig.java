package com.databend.jdbc.internal.session;

import com.databend.jdbc.internal.QueryResultFormat;
import com.databend.jdbc.internal.query.StageAttachment;

import java.util.HashMap;
import java.util.Map;

public class QueryRequestConfig {
    public static final Integer DEFAULT_QUERY_TIMEOUT = 3600;
    public static final Integer DEFAULT_CONNECTION_TIMEOUT = 60;
    public static final Integer DEFAULT_SOCKET_TIMEOUT = 600;
    public static final int DEFAULT_RETRY_ATTEMPTS = 5;
    public static final String X_DATABEND_QUERY_ID = "X-DATABEND-QUERY-ID";
    public static final String X_DATABEND_VERSION = "X-DATABEND-VERSION";
    public static final String X_DATABEND_ROUTE_HINT = "X-DATABEND-ROUTE-HINT";
    public static final String X_DATABEND_STAGE_NAME = "X-DATABEND-STAGE-NAME";
    public static final String X_DATABEND_RELATIVE_PATH = "X-DATABEND-RELATIVE-PATH";
    public static final String X_DATABEND_STICKY_NODE = "X-DATABEND-STICKY-NODE";
    public static final String DATABEND_WAREHOUSE_HEADER = "X-DATABEND-WAREHOUSE";
    public static final String DATABEND_TENANT_HEADER = "X-DATABEND-TENANT";
    public static final String DATABEND_SQL_HEADER = "X-DATABEND-SQL";
    public static final String DATABEND_QUERY_CONTEXT_HEADER = "X-DATABEND-QUERY-CONTEXT";

    private final String host;
    private final SessionState session;
    private final Integer queryTimeoutSecs;
    private final Integer connectionTimeout;
    private final Integer socketTimeout;
    private final QueryResultFormat queryResultFormat;
    private final PaginationOptions paginationOptions;
    private final StageAttachment stageAttachment;
    private final Map<String, String> additionalHeaders;
    private final int retryAttempts;

    public QueryRequestConfig(String host) {
        this(host, SessionState.createDefault(), DEFAULT_QUERY_TIMEOUT, DEFAULT_CONNECTION_TIMEOUT, DEFAULT_SOCKET_TIMEOUT, QueryResultFormat.JSON, PaginationOptions.defaultPaginationOptions(), new HashMap<>(), null, DEFAULT_RETRY_ATTEMPTS);
    }

    public QueryRequestConfig(String host, String database) {
        SessionState session = new SessionState.Builder().setDatabase(database).build();
        this.host = host;
        this.session = session;
        this.queryTimeoutSecs = DEFAULT_QUERY_TIMEOUT;
        this.connectionTimeout = DEFAULT_CONNECTION_TIMEOUT;
        this.socketTimeout = DEFAULT_SOCKET_TIMEOUT;
        this.queryResultFormat = QueryResultFormat.JSON;
        this.paginationOptions = PaginationOptions.defaultPaginationOptions();
        this.additionalHeaders = new HashMap<>();
        this.stageAttachment = null;
        this.retryAttempts = DEFAULT_RETRY_ATTEMPTS;
    }

    public QueryRequestConfig(String host, SessionState session, Integer queryTimeoutSecs, Integer connectionTimeout, Integer socketTimeout, QueryResultFormat queryResultFormat, PaginationOptions paginationOptions, Map<String, String> additionalHeaders, StageAttachment stageAttachment, int retryAttempts) {
        this.host = host;
        this.session = session;
        this.queryTimeoutSecs = queryTimeoutSecs;
        this.connectionTimeout = connectionTimeout;
        this.socketTimeout = socketTimeout;
        this.queryResultFormat = queryResultFormat;
        this.paginationOptions = paginationOptions;
        this.additionalHeaders = additionalHeaders;
        this.stageAttachment = stageAttachment;
        this.retryAttempts = retryAttempts;
    }

    public static Builder builder() {
        return new Builder();
    }

    public SessionState getSession() {
        return session;
    }

    public String getHost() {
        return host;
    }

    public Integer getQueryTimeoutSecs() {
        return queryTimeoutSecs;
    }

    public Integer getConnectionTimeout() {
        return connectionTimeout;
    }

    public Integer getSocketTimeout() {
        return socketTimeout;
    }

    public QueryResultFormat getQueryResultFormat() {
        return queryResultFormat;
    }

    public PaginationOptions getPaginationOptions() {
        return paginationOptions;
    }

    public Map<String, String> getAdditionalHeaders() {
        return additionalHeaders;
    }

    public StageAttachment getStageAttachment() {
        return stageAttachment;
    }

    public int getRetryAttempts() {
        return retryAttempts <= 0 ? DEFAULT_RETRY_ATTEMPTS : retryAttempts;
    }

    public static class Builder {
        private SessionState session;
        private String host;
        private Integer queryTimeoutSecs;
        private Integer connectionTimeout;
        private Integer socketTimeout;
        private QueryResultFormat queryResultFormat = QueryResultFormat.JSON;
        private PaginationOptions paginationOptions;
        private StageAttachment stageAttachment;
        private Map<String, String> additionalHeaders;
        private int retryAttempts;

        public Builder setSession(SessionState session) {
            this.session = session;
            return this;
        }

        public Builder setHost(String host) {
            this.host = host;
            return this;
        }

        public Builder setConnectionTimeout(Integer timeout) {
            this.connectionTimeout = timeout;
            return this;
        }

        public Builder setSocketTimeout(Integer timeout) {
            this.socketTimeout = timeout;
            return this;
        }

        public Builder setQueryResultFormat(QueryResultFormat queryResultFormat) {
            this.queryResultFormat = queryResultFormat;
            return this;
        }

        public Builder setQueryTimeoutSecs(Integer queryTimeoutSecs) {
            if (queryTimeoutSecs <= 0) {
                queryTimeoutSecs = DEFAULT_QUERY_TIMEOUT;
            }
            this.queryTimeoutSecs = queryTimeoutSecs;
            return this;
        }

        public Builder setPaginationOptions(PaginationOptions paginationOptions) {
            this.paginationOptions = paginationOptions;
            return this;
        }

        public Builder setAdditionalHeaders(Map<String, String> additionalHeaders) {
            this.additionalHeaders = additionalHeaders;
            return this;
        }

        public Builder setRetryAttempts(int retryAttempts) {
            this.retryAttempts = retryAttempts;
            return this;
        }

        public Builder setStageAttachment(StageAttachment stageAttachment) {
            this.stageAttachment = stageAttachment;
            return this;
        }

        public QueryRequestConfig build() {
            return new QueryRequestConfig(host, session, queryTimeoutSecs, connectionTimeout, socketTimeout, queryResultFormat, paginationOptions, additionalHeaders, stageAttachment, retryAttempts);
        }
    }
}
