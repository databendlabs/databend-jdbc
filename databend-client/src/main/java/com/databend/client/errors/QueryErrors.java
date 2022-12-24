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

package com.databend.client.errors;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * QueryErrors represent errors from databend server
 */
public class QueryErrors
{
    private final int code;
    private final String message;

    @JsonCreator
    public QueryErrors(
            @JsonProperty("code") int code,
            @JsonProperty("message") String message)
    {
        this.code = code;
        this.message = message;
    }

    // add builder
    public static Builder builder() {
        return new Builder();
    }

    @JsonProperty
    public int getCode() {
        return code;
    }

    @JsonProperty
    public String getMessage() {
        return message;
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("code", code)
                .add("message", message)
                .toString();
    }

    public static final class Builder {
        private int code;
        private String message;

        public Builder setCode(int code) {
            this.code = code;
            return this;
        }

        public Builder setMessage(String message) {
            this.message = message;
            return this;
        }

        public QueryErrors build() {
            return new QueryErrors(code, message);
        }
    }
}
