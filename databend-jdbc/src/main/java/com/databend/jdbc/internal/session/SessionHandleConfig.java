package com.databend.jdbc.internal.session;

import java.net.URI;
import java.util.Objects;

public final class SessionHandleConfig {
    private final URI baseUri;
    private final Integer queryTimeoutSecs;
    private final Integer connectionTimeoutSecs;
    private final Integer socketTimeoutSecs;
    private final Integer waitTimeSecs;
    private final Integer maxRowsInBuffer;
    private final Integer maxRowsPerPage;
    private final String warehouse;
    private final String tenant;
    private final boolean debug;
    private final SessionState initialSession;

    private SessionHandleConfig(Builder builder) {
        this.baseUri = Objects.requireNonNull(builder.baseUri, "baseUri is null");
        this.queryTimeoutSecs = builder.queryTimeoutSecs;
        this.connectionTimeoutSecs = builder.connectionTimeoutSecs;
        this.socketTimeoutSecs = builder.socketTimeoutSecs;
        this.waitTimeSecs = builder.waitTimeSecs;
        this.maxRowsInBuffer = builder.maxRowsInBuffer;
        this.maxRowsPerPage = builder.maxRowsPerPage;
        this.warehouse = builder.warehouse;
        this.tenant = builder.tenant;
        this.debug = builder.debug;
        this.initialSession = Objects.requireNonNull(builder.initialSession, "initialSession is null");
    }

    public static Builder builder() {
        return new Builder();
    }

    public URI getBaseUri() {
        return baseUri;
    }

    public Integer getQueryTimeoutSecs() {
        return queryTimeoutSecs;
    }

    public Integer getConnectionTimeoutSecs() {
        return connectionTimeoutSecs;
    }

    public Integer getSocketTimeoutSecs() {
        return socketTimeoutSecs;
    }

    public Integer getWaitTimeSecs() {
        return waitTimeSecs;
    }

    public Integer getMaxRowsInBuffer() {
        return maxRowsInBuffer;
    }

    public Integer getMaxRowsPerPage() {
        return maxRowsPerPage;
    }

    public String getWarehouse() {
        return warehouse;
    }

    public String getTenant() {
        return tenant;
    }

    public boolean isDebug() {
        return debug;
    }

    public SessionState getInitialSession() {
        return initialSession;
    }

    public static final class Builder {
        private URI baseUri;
        private Integer queryTimeoutSecs;
        private Integer connectionTimeoutSecs;
        private Integer socketTimeoutSecs;
        private Integer waitTimeSecs;
        private Integer maxRowsInBuffer;
        private Integer maxRowsPerPage;
        private String warehouse;
        private String tenant;
        private boolean debug;
        private SessionState initialSession;

        public Builder setBaseUri(URI baseUri) {
            this.baseUri = baseUri;
            return this;
        }

        public Builder setQueryTimeoutSecs(Integer queryTimeoutSecs) {
            this.queryTimeoutSecs = queryTimeoutSecs;
            return this;
        }

        public Builder setConnectionTimeoutSecs(Integer connectionTimeoutSecs) {
            this.connectionTimeoutSecs = connectionTimeoutSecs;
            return this;
        }

        public Builder setSocketTimeoutSecs(Integer socketTimeoutSecs) {
            this.socketTimeoutSecs = socketTimeoutSecs;
            return this;
        }

        public Builder setWaitTimeSecs(Integer waitTimeSecs) {
            this.waitTimeSecs = waitTimeSecs;
            return this;
        }

        public Builder setMaxRowsInBuffer(Integer maxRowsInBuffer) {
            this.maxRowsInBuffer = maxRowsInBuffer;
            return this;
        }

        public Builder setMaxRowsPerPage(Integer maxRowsPerPage) {
            this.maxRowsPerPage = maxRowsPerPage;
            return this;
        }

        public Builder setWarehouse(String warehouse) {
            this.warehouse = warehouse;
            return this;
        }

        public Builder setTenant(String tenant) {
            this.tenant = tenant;
            return this;
        }

        public Builder setDebug(boolean debug) {
            this.debug = debug;
            return this;
        }

        public Builder setInitialSession(SessionState initialSession) {
            this.initialSession = initialSession;
            return this;
        }

        public SessionHandleConfig build() {
            return new SessionHandleConfig(this);
        }
    }
}
