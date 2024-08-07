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

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Random;

public class ClientSettings {
    public static final Integer DEFAULT_QUERY_TIMEOUT = 300;
    public static final Integer DEFAULT_CONNECTION_TIMEOUT = 0; // seconds
    public static final Integer DEFAULT_SOCKET_TIMEOUT = 0;
    public static final int DEFAULT_RETRY_ATTEMPTS = 5;
    public static final String X_Databend_Query_ID = "X-DATABEND-QUERY-ID";
    public static final String X_DATABEND_ROUTE_HINT = "X-DATABEND-ROUTE-HINT";
    public static final String DatabendWarehouseHeader = "X-DATABEND-WAREHOUSE";
    public static final String DatabendTenantHeader = "X-DATABEND-TENANT";
    private final String host;
    private final DatabendSession session;
    private final Integer queryTimeoutSecs;
    private final Integer connectionTimeout;
    private final Integer socketTimeout;

    private final PaginationOptions paginationOptions;

    private final StageAttachment stageAttachment;
    private Map<String, String> additionalHeaders;

    private final int retryAttempts;
    // TODO(zhihanz) timezone and locale info

    public ClientSettings(String host) {
        this(host, DatabendSession.createDefault(), DEFAULT_QUERY_TIMEOUT, DEFAULT_CONNECTION_TIMEOUT, DEFAULT_SOCKET_TIMEOUT, PaginationOptions.defaultPaginationOptions(), new HashMap<String, String>(), null, DEFAULT_RETRY_ATTEMPTS);
    }

    public ClientSettings(String host, String database) {
        DatabendSession session = new DatabendSession.Builder().setDatabase(database).build();
        this.host = host;
        this.session = session;
        this.queryTimeoutSecs = DEFAULT_QUERY_TIMEOUT;
        this.connectionTimeout = DEFAULT_CONNECTION_TIMEOUT;
        this.socketTimeout = DEFAULT_SOCKET_TIMEOUT;
        this.paginationOptions = PaginationOptions.defaultPaginationOptions();
        this.additionalHeaders = new HashMap<>();
        this.stageAttachment = null;
        this.retryAttempts = DEFAULT_RETRY_ATTEMPTS;
    }

    public ClientSettings(String host, DatabendSession session,
                          Integer queryTimeoutSecs,
                          Integer connectionTimeout,
                          Integer socketTimeout,
                          PaginationOptions paginationOptions,
                          Map<String, String> additionalHeaders,
                          StageAttachment stageAttachment,
                          int retryAttempts) {
        this.host = host;
        this.session = session;
        this.queryTimeoutSecs = queryTimeoutSecs;
        this.connectionTimeout = connectionTimeout;
        this.socketTimeout = socketTimeout;
        this.paginationOptions = paginationOptions;
        this.additionalHeaders = additionalHeaders;
        this.stageAttachment = stageAttachment;
        this.retryAttempts = retryAttempts;
    }

    public static Builder builder() {
        return new Builder();
    }

    public DatabendSession getSession() {
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
        if (retryAttempts <= 0) {
            return DEFAULT_RETRY_ATTEMPTS;
        }
        return retryAttempts;
    }

    public static class Builder {
        private DatabendSession session;
        private String host;
        private Integer queryTimeoutSecs;
        private Integer connectionTimeout;
        private Integer socketTimeout;

        private PaginationOptions paginationOptions;
        private StageAttachment stageAttachment;
        private Map<String, String> additionalHeaders;

        private int retryAttempts;

        public Builder setSession(DatabendSession session) {
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

        public ClientSettings build() {
            return new ClientSettings(host, session, queryTimeoutSecs, connectionTimeout, socketTimeout, paginationOptions, additionalHeaders, stageAttachment, retryAttempts);
        }
    }

}
