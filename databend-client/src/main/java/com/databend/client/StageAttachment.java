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

import java.util.Map;

import static com.google.common.base.MoreObjects.toStringHelper;

public class StageAttachment
{
    /// location of the stage
    /// for example: @stage_name/path/to/file, @~/path/to/file
    private final String location;
    private final Map<String, String> fileFormatOptions;
    private final Map<String, String> copyOptions;

    @JsonCreator
    public StageAttachment(@JsonProperty("location") String location,
            @JsonProperty("file_format_options") Map<String, String> fileFormatOptions,
            @JsonProperty("copy_options") Map<String, String> copyOptions)
    {
        this.location = location;
        this.fileFormatOptions = fileFormatOptions;
        this.copyOptions = copyOptions;
    }

    // add builder
    public static Builder builder()
    {
        return new Builder();
    }

    @JsonProperty
    public String getLocation()
    {
        return location;
    }

    @JsonProperty
    public Map<String, String> getFileFormatOptions()
    {
        return fileFormatOptions;
    }

    @JsonProperty
    public Map<String, String> getCopyOptions()
    {
        return copyOptions;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("location", location)
                .add("file_format_options", fileFormatOptions)
                .add("copy_options", copyOptions)
                .toString();
    }

    public static final class Builder
    {
        private String location;
        private Map<String, String> fileFormatOptions;
        private Map<String, String> copyOptions;

        public Builder setLocation(String location)
        {
            this.location = location;
            return this;
        }

        public Builder setFileFormatOptions(Map<String, String> fileFormatOptions)
        {
            this.fileFormatOptions = fileFormatOptions;
            return this;
        }

        public Builder setCopyOptions(Map<String, String> copyOptions)
        {
            this.copyOptions = copyOptions;
            return this;
        }

        public StageAttachment build()
        {
            return new StageAttachment(location, fileFormatOptions, copyOptions);
        }
    }
}
