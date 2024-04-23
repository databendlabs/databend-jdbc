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

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Databend client session configuration.
 */
public class DatabendSession {
    private static final String DEFAULT_DATABASE = "default";
    private static final String AUTO_COMMIT = "AutoCommit";

    private final String database;
    private final AtomicBoolean autoCommit = new AtomicBoolean(false);


    private final Map<String, String> settings;

    // txn
    private String txnState;

    private Map<String, Object> additionalProperties = new HashMap<>();

    @JsonCreator
    public DatabendSession(
            @JsonProperty("database") String database,
            @JsonProperty("settings") Map<String, String> settings,
            @JsonProperty("txn_state") String txnState) {
        this.database = database;
        this.settings = settings;
        this.txnState = txnState;
    }

    // default
    public static DatabendSession createDefault() {
        return new DatabendSession(DEFAULT_DATABASE, null, null);
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

    @JsonAnyGetter
    public Map<String, Object> getAdditionalProperties() {
        return additionalProperties;
    }

    @JsonAnySetter
    public void setAdditionalProperty(String key, Object value) {
        additionalProperties.put(key, value);
    }

    @Override
    public String toString() {
        return toStringHelper(this).add("database", database).add("settings", settings).toString();
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

    public static final class Builder {
        private URI host;
        private String database;
        private final AtomicBoolean autoCommit = new AtomicBoolean(false);
        private Map<String, String> settings;

        // txn
        private String txnState;

        public Builder setHost(URI host) {
            this.host = host;
            return this;
        }

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

        public DatabendSession build() {
            return new DatabendSession(database, settings, txnState);
        }
    }
}
