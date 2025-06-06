package com.databend.client;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class ServerInfo {
    private final String id;
    private final String startTime;

    @JsonCreator
    public ServerInfo(
            @JsonProperty("id") String id,
            @JsonProperty("start_time") String startTime) {
        this.id = id;
        this.startTime = startTime;
    }

    @JsonProperty
    public String getId() {
        return id;
    }

    @JsonProperty("start_time")
    public String getStartTime() {
        return startTime;
    }

    @JsonProperty
    @Override
    public String toString() {
        return "ServerInfo{" +
                "id='" + id + '\'' +
                ", startTime='" + startTime + '\'' +
                '}';
    }
}
