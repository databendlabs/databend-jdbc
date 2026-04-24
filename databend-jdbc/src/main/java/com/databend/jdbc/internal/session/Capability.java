package com.databend.jdbc.internal.session;

import com.vdurmont.semver4j.Semver;

public class Capability {
    private final boolean streamingLoad;
    private final boolean heartbeat;
    private final boolean arrowData;
    public Capability(Semver ver) {
         streamingLoad =  ver.isGreaterThan(new Semver("1.2.781"));
         heartbeat = ver.isGreaterThan(new Semver("1.2.709"));
         arrowData = ver.isGreaterThan(new Semver("1.2.898"));
    }

    public boolean streamingLoad()  {
        return streamingLoad;
    }

    public boolean heartBeat() {
        return heartbeat;
    }

    public boolean arrowData() {
        return arrowData;
    }
}
