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


import static com.google.common.base.MoreObjects.toStringHelper;

public class DiscoveryNode {
    private final String address;

    @JsonCreator
    public DiscoveryNode(
            @JsonProperty("address") String address) {
        this.address = address;
    }

    public static DiscoveryNode create(String address) {
        return new DiscoveryNode(address);
    }
    // add builder

    @JsonProperty
    public String getAddress() {
        return address;
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("address", address)
                .toString();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private String address;

        public Builder setAddress(String address) {
            this.address = address;
            return this;
        }

        public DiscoveryNode build() {
            return new DiscoveryNode(address);
        }
    }
}
