package com.databend.jdbc.internal.query;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.concurrent.Immutable;

import static com.google.common.base.MoreObjects.toStringHelper;

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
                .add("scanProgress", scanProgress)
                .add("writeProgress", writeProgress)
                .add("readProgress", resultProgress)
                .toString();
    }
}
