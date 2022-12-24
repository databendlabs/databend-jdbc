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

import java.net.URI;
import java.util.Map;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Databend client session configuration.
 *
 */
public class DatabendSession {
    private static final String DEFAULT_DATABASE = "default";

    // always ensure that session query id could be retrieved under reasonable time
    private static final int DEFAULT_SESSION_ALIVE_SECS = 30;

    // the
    private final String database;

    private final int keepServerSessionSecs;

    private final Map<String, String> settings;


    @JsonCreator
    public DatabendSession(
            @JsonProperty("database") String database,
            @JsonProperty("keep_server_session_secs") int keepServerSessionSecs,
            @JsonProperty("settings") Map<String, String> settings) {
        this.database = database;
        this.keepServerSessionSecs = keepServerSessionSecs;
        this.settings = settings;
    }

    // default
    public static DatabendSession createDefault() {
        return new DatabendSession(DEFAULT_DATABASE, DEFAULT_SESSION_ALIVE_SECS, null);
    }

    public static Builder builder() {
        return new Builder();
    }

    @JsonProperty
    public String getDatabase() {
        return database;
    }

    @JsonProperty
    public int getKeepServerSessionSecs() {
        return keepServerSessionSecs;
    }

    @JsonProperty
    public Map<String, String> getSettings() {
        return settings;
    }

    @Override
    public String toString() {
        return toStringHelper(this).add("database", database).add("keepServerSessionSecs", keepServerSessionSecs).add("settings", settings).toString();
    }

    public static final class Builder {
        private URI host;
        private String database;
        private int keepServerSessionSecs = 30;
        private Map<String, String> settings;

        public Builder setHost(URI host) {
            this.host = host;
            return this;
        }

        public Builder setDatabase(String database) {
            this.database = database;
            return this;
        }

        public Builder setKeepServerSessionSecs(int keepServerSessionSecs) {
            this.keepServerSessionSecs = keepServerSessionSecs;
            return this;
        }

        public Builder setSettings(Map<String, String> settings) {
            this.settings = settings;
            return this;
        }

        public DatabendSession build() {
            return new DatabendSession(database, keepServerSessionSecs, settings);
        }
    }


}
