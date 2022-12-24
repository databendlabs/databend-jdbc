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
import com.fasterxml.jackson.databind.ObjectMapper;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 *  CloudErrors is a list of errors that can be returned by the databend cloud service.
 */
public class CloudErrors
{
    private final String kind;
    private final String message;

    @JsonCreator
    public CloudErrors(
            @JsonProperty("kind") String kind,
            @JsonProperty("message") String message)
    {
        this.kind = kind;
        this.message = message;
    }

    // return null if parse failed
    public static CloudErrors tryParse(String json)
    {
        try {
            ObjectMapper mapper = new ObjectMapper();
            return mapper.readValue(json, CloudErrors.class);
        } catch (Exception e) {
            return null;
        }
    }

    @JsonProperty
    public String getKind() {
        return kind;
    }

    @JsonProperty
    public String getMessage() {
        return message;
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("kind", kind)
                .add("message", message)
                .toString();
    }

    public CloudErrorKinds tryGetErrorKind() {
        return CloudErrorKinds.tryGetErrorKind(kind);
    }
}
