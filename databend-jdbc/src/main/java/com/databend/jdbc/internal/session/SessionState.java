package com.databend.jdbc.internal.session;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.MoreObjects.toStringHelper;

public class SessionState {
    private static final String DEFAULT_DATABASE = "default";
    private static final String AUTO_COMMIT = "AutoCommit";
    private static final String TXN_STATE_ACTIVE = "Active";

    private final String database;
    private final AtomicBoolean autoCommit = new AtomicBoolean(false);
    private final Map<String, String> settings;
    private String txnState;
    private Boolean needSticky;
    private Boolean needKeepAlive;
    private Map<String, Object> additionalProperties = new HashMap<>();

    @JsonCreator
    public SessionState(
            @JsonProperty("database") String database,
            @JsonProperty("settings") Map<String, String> settings,
            @JsonProperty("txn_state") String txnState,
            @JsonProperty("need_sticky") Boolean needSticky,
            @JsonProperty("need_keep_alive") Boolean needKeepAlive) {
        this.database = database;
        this.settings = settings;
        this.txnState = txnState;
        this.needSticky = needSticky != null ? needSticky : false;
        this.needKeepAlive = needKeepAlive != null ? needKeepAlive : false;
    }

    public static SessionState createDefault() {
        return new SessionState(DEFAULT_DATABASE, null, null, false, false);
    }

    public static Builder builder() {
        return new Builder();
    }

    @JsonProperty
    public String getDatabase() {
        return database;
    }

    @JsonProperty
    public Map<String, String> getSettings() {
        return settings;
    }

    @JsonProperty("txn_state")
    public String getTxnState() {
        return txnState;
    }

    @JsonProperty("need_sticky")
    public Boolean getNeedSticky() {
        return needSticky;
    }

    @JsonProperty("need_keep_alive")
    public Boolean getNeedKeepAlive() {
        return needKeepAlive;
    }

    @JsonAnyGetter
    public Map<String, Object> getAdditionalProperties() {
        return additionalProperties;
    }

    @JsonAnySetter
    public void setAdditionalProperty(String key, Object value) {
        additionalProperties.put(key, value);
    }

    public boolean getAutoCommit() {
        return autoCommit.get();
    }

    public boolean inActiveTransaction() {
        return txnState != null && txnState.equals(TXN_STATE_ACTIVE);
    }

    public void setAutoCommit(boolean autoCommit) {
        this.autoCommit.set(autoCommit);
        if (autoCommit) {
            this.txnState = AUTO_COMMIT;
        }
    }

    @Override
    public String toString() {
        return toStringHelper(this).add("database", database).add("settings", settings).toString();
    }

    public static final class Builder {
        private String database;
        private final AtomicBoolean autoCommit = new AtomicBoolean(false);
        private Map<String, String> settings;
        private String txnState;

        public Builder setDatabase(String database) {
            this.database = database;
            return this;
        }

        public Builder setSettings(Map<String, String> settings) {
            this.settings = settings;
            return this;
        }

        public Builder setTxnState(String txnState) {
            this.txnState = txnState;
            return this;
        }

        public boolean getAutoCommit() {
            return autoCommit.get();
        }

        public void setAutoCommit(boolean autoCommit) {
            this.autoCommit.set(autoCommit);
            if (autoCommit) {
                this.txnState = AUTO_COMMIT;
            }
        }

        public SessionState build() {
            return new SessionState(database, settings, txnState, false, false);
        }
    }
}
