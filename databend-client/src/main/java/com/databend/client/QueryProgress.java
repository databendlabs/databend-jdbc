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

import java.math.BigInteger;

import static com.google.common.base.MoreObjects.toStringHelper;

public class QueryProgress {
    private final BigInteger rows;
    private final BigInteger bytes;

    @JsonCreator
    public QueryProgress(
            @JsonProperty("rows") BigInteger rows,
            @JsonProperty("bytes") BigInteger bytes) {
        this.rows = rows;
        this.bytes = bytes;
    }

    // add builder
    @JsonProperty
    public BigInteger getRows() {
        return rows;
    }

    @JsonProperty
    public BigInteger getBytes() {
        return bytes;
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("rows", rows)
                .add("bytes", bytes)
                .toString();
    }
}
