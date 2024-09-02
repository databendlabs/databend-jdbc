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

public class PaginationOptions {
    private static final int DEFAULT_WAIT_TIME_SEC = 10;
    private static final int DEFAULT_MAX_ROWS_IN_BUFFER = 5 * 1000 * 1000;

    private static final int DEFAULT_MAX_ROWS_PER_PAGE = 10000;

    private final int waitTimeSecs;
    private final int maxRowsInBuffer;
    private final int maxRowsPerPage;

    @JsonCreator
    public PaginationOptions(
            @JsonProperty("wait_time_secs") int waitTimeSecs
            , @JsonProperty("max_rows_in_buffer") int maxRowsInBuffer
            , @JsonProperty("max_rows_per_page") int maxRowsPerPage
    ) {
        this.waitTimeSecs = waitTimeSecs;
        this.maxRowsInBuffer = maxRowsInBuffer;
        this.maxRowsPerPage = maxRowsPerPage;
    }

    // default set wait time secs to 10s
    public static PaginationOptions defaultPaginationOptions() {
        return new PaginationOptions(DEFAULT_WAIT_TIME_SEC, DEFAULT_MAX_ROWS_IN_BUFFER, DEFAULT_MAX_ROWS_PER_PAGE);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static int getDefaultWaitTimeSec() {
        return DEFAULT_WAIT_TIME_SEC;
    }

    public static int getDefaultMaxRowsInBuffer() {
        return DEFAULT_MAX_ROWS_IN_BUFFER;
    }

    public static int getDefaultMaxRowsPerPage() {
        return DEFAULT_MAX_ROWS_PER_PAGE;
    }

    @JsonProperty("wait_time_secs")
    public int getWaitTimeSecs() {
        return waitTimeSecs;
    }

    @JsonProperty("max_rows_in_buffer")
    public int getMaxRowsInBuffer() {
        return maxRowsInBuffer;
    }

    @JsonProperty("max_rows_per_page")
    public int getMaxRowsPerPage() {
        return maxRowsPerPage;
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("waitTimeSecs", waitTimeSecs)
                .add("maxRowsInBuffer", maxRowsInBuffer)
                .add("maxRowsPerPage", maxRowsPerPage)
                .toString();
    }

    public static class Builder {
        private int waitTimeSecs;

        private int maxRowsInBuffer;

        private int maxRowsPerPage;

        public Builder setWaitTimeSecs(int waitTimeSecs) {
            this.waitTimeSecs = waitTimeSecs;
            return this;
        }

        public Builder setMaxRowsInBuffer(int maxRowsInBuffer) {
            this.maxRowsInBuffer = maxRowsInBuffer;
            return this;
        }

        public Builder setMaxRowsPerPage(int maxRowsPerPage) {
            this.maxRowsPerPage = maxRowsPerPage;
            return this;
        }


        public PaginationOptions build() {
            return new PaginationOptions(waitTimeSecs, maxRowsInBuffer, maxRowsPerPage);
        }
    }
}
