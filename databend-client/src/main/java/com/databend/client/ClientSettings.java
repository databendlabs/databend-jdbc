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

import java.time.Duration;
import java.util.Map;

public class ClientSettings {
    public static final Duration DEFAULT_QUERY_TIMEOUT = Duration.ofSeconds(60);
    public static final Integer DEFAULT_CONNECTION_TIMEOUT = 15; // seconds
    public static final int DEFAULT_RETRY_ATTEMPTS = 5;
    private final String host;
    private final DatabendSession session;
    private final Duration queryTimeoutNanos;
    private final Integer connectionTimeout;

    private final PaginationOptions paginationOptions;

    private final StageAttachment stageAttachment;
    private final Map<String, String> additionalHeaders;

    private final int retryAttempts;
    // TODO(zhihanz) timezone and locale info

    public ClientSettings(String host) {
        this(host, DatabendSession.createDefault(), DEFAULT_QUERY_TIMEOUT, DEFAULT_CONNECTION_TIMEOUT, PaginationOptions.defaultPaginationOptions(), null, null, DEFAULT_RETRY_ATTEMPTS);
    }

    public ClientSettings(String host, String database) {
        DatabendSession session = new DatabendSession.Builder().setDatabase(database).build();
        this.host = host;
        this.session = session;
        this.queryTimeoutNanos = DEFAULT_QUERY_TIMEOUT;
        this.connectionTimeout = DEFAULT_CONNECTION_TIMEOUT;
        this.paginationOptions = PaginationOptions.defaultPaginationOptions();
        this.additionalHeaders = null;
        this.stageAttachment = null;
        this.retryAttempts = DEFAULT_RETRY_ATTEMPTS;
    }

    public ClientSettings(String host, DatabendSession session,
                          Duration queryTimeoutNanos,
                          Integer connectionTimeout,
                          PaginationOptions paginationOptions,
                          Map<String, String> additionalHeaders,
                          StageAttachment stageAttachment,
                          int retryAttempts) {
        this.host = host;
        this.session = session;
        this.queryTimeoutNanos = queryTimeoutNanos;
        this.connectionTimeout = connectionTimeout;
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

    public Duration getQueryTimeoutNanos() {
        return queryTimeoutNanos;
    }

    public Integer getConnectionTimeout() {
        return connectionTimeout;
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
        private Duration queryTimeoutNanos;
        private Integer connectionTimeout;

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

        public Builder setQueryTimeoutNanos(Duration queryTimeoutNanos) {
            this.queryTimeoutNanos = queryTimeoutNanos;
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
            return new ClientSettings(host, session, queryTimeoutNanos, connectionTimeout, paginationOptions, additionalHeaders, stageAttachment, retryAttempts);
        }
    }

}
