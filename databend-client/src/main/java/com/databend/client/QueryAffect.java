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
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = QueryAffect.Create.class, name = "Create"),
        @JsonSubTypes.Type(value = QueryAffect.UseDB.class, name = "UseDB"),
        @JsonSubTypes.Type(value = QueryAffect.ChangeSettings.class, name = "ChangeSettings")
})
public abstract class QueryAffect {

    // TODO(zhihanz): currently create affect have not been implemented
    public static class Create extends QueryAffect {
        private final String kind;
        private final String name;
        private final boolean success;

        @JsonCreator
        public Create(
                @JsonProperty("kind") String kind,
                @JsonProperty("name") String name,
                @JsonProperty("success") boolean success
        ) {
            this.kind = kind;
            this.name = name;
            this.success = success;
        }

        // add builder
        public static Builder builder() {
            return new Builder();
        }

        @JsonProperty
        public String getKind() {
            return kind;
        }

        @JsonProperty
        public String getName() {
            return name;
        }

        @JsonProperty
        public boolean isSuccess() {
            return success;
        }

        @Override
        public String toString() {
            return toStringHelper(this)
                    .add("kind", kind)
                    .add("name", name)
                    .add("success", success)
                    .toString();
        }

        public static final class Builder {
            private String kind;
            private String name;
            private boolean success;

            public Builder setKind(String kind) {
                this.kind = kind;
                return this;
            }

            public Builder setName(String name) {
                this.name = name;
                return this;
            }

            public Builder setSuccess(boolean success) {
                this.success = success;
                return this;
            }

            public Create build() {
                return new Create(kind, name, success);
            }
        }
    }

    public static class UseDB extends QueryAffect {
        private final String name;

        @JsonCreator
        public UseDB(@JsonProperty("name") String name) {
            this.name = name;
        }

        // add builder
        public static Builder builder() {
            return new Builder();
        }

        @JsonProperty
        public String getName() {
            return name;
        }

        @Override
        public String toString() {
            return toStringHelper(this)
                    .add("name", name)
                    .toString();
        }

        public static final class Builder {
            private String name;

            public Builder setName(String name) {
                this.name = name;
                return this;
            }

            public UseDB build() {
                return new UseDB(name);
            }
        }
    }

    public static class ChangeSettings extends QueryAffect {
        private final List<String> keys;
        private final List<String> values;
        private final List<Boolean> isGlobals;

        @JsonCreator
        public ChangeSettings(
                @JsonProperty("keys") List<String> keys,
                @JsonProperty("values") List<String> values,
                @JsonProperty("is_globals") List<Boolean> isGlobals
        ) {
            this.keys = keys;
            this.values = values;
            this.isGlobals = isGlobals;
        }

        // add builder
        public static Builder builder() {
            return new Builder();
        }

        @JsonProperty
        public List<String> getKeys() {
            return keys;
        }

        @JsonProperty
        public List<String> getValues() {
            return values;
        }

        @JsonProperty
        public List<Boolean> getIsGlobals() {
            return isGlobals;
        }

        @Override
        public String toString() {
            return toStringHelper(this)
                    .add("keys", keys)
                    .add("values", values)
                    .add("isGlobals", isGlobals)
                    .toString();
        }

        public static final class Builder {
            private List<String> keys;
            private List<String> values;
            private List<Boolean> isGlobals;

            public Builder setKeys(List<String> keys) {
                this.keys = keys;
                return this;
            }

            public Builder setValues(List<String> values) {
                this.values = values;
                return this;
            }

            public Builder setIsGlobals(List<Boolean> isGlobals) {
                this.isGlobals = isGlobals;
                return this;
            }

            public ChangeSettings build() {
                return new ChangeSettings(keys, values, isGlobals);
            }
        }
    }
}
