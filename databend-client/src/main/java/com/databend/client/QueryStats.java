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

import javax.annotation.concurrent.Immutable;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * QueryStats contain the statistics of a query.
 * full doc: https://databend.rs/doc/integrations/api/rest#query-response
 */
@Immutable
public class QueryStats {
    private final float runningTimeMS;
    private final QueryProgress scanProgress;
    private final QueryProgress writeProgress;
    private final QueryProgress resultProgress;
    @JsonCreator
    public QueryStats(
            @JsonProperty("running_time_ms") float runningTimeMS,
            @JsonProperty("scan_progress") QueryProgress scanProgress,
            @JsonProperty("write_progress") QueryProgress writeProgress,
            @JsonProperty("result_progress") QueryProgress resultProgress) {
        this.runningTimeMS = runningTimeMS;
        this.scanProgress = scanProgress;
        this.writeProgress = writeProgress;
        this.resultProgress = resultProgress;
    }

    // add builder
    @JsonProperty
    public float getRunningTimeMS() {
        return runningTimeMS;
    }

    @JsonProperty
    public QueryProgress getScanProgress() {
        return scanProgress;
    }

    @JsonProperty
    public QueryProgress getWriteProgress() {
        return writeProgress;
    }

    @JsonProperty
    public QueryProgress getResultProgress() {
        return resultProgress;
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("runningTimeMS", runningTimeMS)
                .add("scamProgress", scanProgress)
                .add("writeProgress", writeProgress)
                .add("readProgress", resultProgress)
                .toString();
    }
}


