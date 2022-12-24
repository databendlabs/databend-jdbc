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

import java.util.List;
import java.util.Map;

import static com.google.common.base.MoreObjects.toStringHelper;

public class QuerySchema {
    private final List<QueryRowField> fields;
    private final Map<String, String> metadata;

    @JsonCreator
    public QuerySchema(
            @JsonProperty("fields") List<QueryRowField> fields,
            @JsonProperty("metadata") Map<String, String> metadata) {
        this.fields = fields;
        this.metadata = metadata;
    }

    // add builder
    public static Builder builder() {
        return new Builder();
    }

    @JsonProperty
    public List<QueryRowField> getFields() {
        return fields;
    }

    @JsonProperty
    public Map<String, String> getMetadata() {
        return metadata;
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("fields", fields)
                .add("metadata", metadata)
                .toString();
    }

    public static final class Builder {
        private List<QueryRowField> fields;
        private Map<String, String> metadata;

        public Builder setFields(List<QueryRowField> fields) {
            this.fields = fields;
            return this;
        }

        public Builder setMetadata(Map<String, String> metadata) {
            this.metadata = metadata;
            return this;
        }

        public QuerySchema build() {
            return new QuerySchema(fields, metadata);
        }
    }

}
