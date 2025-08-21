package com.databend.jdbc;

import com.vdurmont.semver4j.Semver;

public class Capability {
    private final boolean streamingLoad;
    private final boolean heartbeat;
    public Capability(Semver ver) {
         streamingLoad =  ver.isGreaterThan(new Semver("1.2.781"));
         heartbeat = ver.isGreaterThan(new Semver("1.2.709"));
    }

    public boolean streamingLoad()  {
        return streamingLoad;
    }

    public boolean heartBeat() {
        return heartbeat;
    }
}
