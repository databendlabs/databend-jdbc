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

import com.databend.client.data.DatabendRawType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static com.google.common.base.MoreObjects.toStringHelper;

public class QueryRowField {
    private final String name;
    /// default_expr is serialized representation from PlanExpression
    private final String defaultExpr;

    // possible it contain nullable wrapper
    private final DatabendRawType DataType;
    @JsonCreator
    public QueryRowField(
            @JsonProperty("name") String name,
            @JsonProperty("default_expr") String defaultExpr,
            @JsonProperty("data_type") DatabendRawType DataType) {
        this.name = name;
        this.defaultExpr = defaultExpr;
        this.DataType = DataType;
    }

    // add builder
    public static Builder builder() {
        return new Builder();
    }

    @JsonProperty
    public String getName() {
        return name;
    }

    @JsonProperty
    public String getDefaultExpr() {
        return defaultExpr;
    }

    @JsonProperty
    public DatabendRawType getDataType() {
        return DataType;
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("name", name)
                .add("defaultExpr", defaultExpr)
                .add("DataType", DataType)
                .toString();
    }

    public static final class Builder {
        private String name;
        private String defaultExpr;
        private DatabendRawType DataType;

        public Builder setName(String name) {
            this.name = name;
            return this;
        }

        public Builder setDefaultExpr(String defaultExpr) {
            this.defaultExpr = defaultExpr;
            return this;
        }

        public Builder setDataType(DatabendRawType DataType) {
            this.DataType = DataType;
            return this;
        }

        public QueryRowField build() {
            return new QueryRowField(name, defaultExpr, DataType);
        }
    }


}
