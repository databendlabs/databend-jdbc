package com.databend.jdbc.internal.query;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.MoreObjects.toStringHelper;

public class StageAttachment {
    public static final String DEFAULT_FILE_FORMAT = "CSV";

    private final String location;
    private final Map<String, String> fileFormatOptions;
    private final Map<String, String> copyOptions;

    @JsonCreator
    public StageAttachment(
            @JsonProperty("location") String location,
            @JsonProperty("file_format_options") Map<String, String> fileFormatOptions,
            @JsonProperty("copy_options") Map<String, String> copyOptions) {
        this.location = location;
        this.fileFormatOptions = fileFormatOptions == null ? new HashMap<>() : fileFormatOptions;
        this.fileFormatOptions.put("type", DEFAULT_FILE_FORMAT);
        this.copyOptions = copyOptions;
    }

    public static Builder builder() {
        return new Builder();
    }

    @JsonProperty("location")
    public String getLocation() {
        return location;
    }

    @JsonProperty("file_format_options")
    public Map<String, String> getFileFormatOptions() {
        return fileFormatOptions;
    }

    @JsonProperty("copy_options")
    public Map<String, String> getCopyOptions() {
        return copyOptions;
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("location", location)
                .add("file_format_options", fileFormatOptions)
                .add("copy_options", copyOptions)
                .toString();
    }

    public static final class Builder {
        private String location;
        private Map<String, String> fileFormatOptions;
        private Map<String, String> copyOptions;

        public Builder setLocation(String location) {
            this.location = location;
            return this;
        }

        public Builder setFileFormatOptions(Map<String, String> fileFormatOptions) {
            this.fileFormatOptions = fileFormatOptions;
            return this;
        }

        public Builder setCopyOptions(Map<String, String> copyOptions) {
            this.copyOptions = copyOptions;
            return this;
        }

        public StageAttachment build() {
            return new StageAttachment(location, fileFormatOptions, copyOptions);
        }
    }
}
