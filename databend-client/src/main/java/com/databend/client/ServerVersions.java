package com.databend.client;

import com.github.zafarkhaja.semver.Version;

public class ServerVersions {
    private static final Version HEARTBEAT = Version.forIntegers(1, 2, 709);

    public static boolean supportHeartbeat(Version ver) {
        return ver != null && ver.greaterThan(HEARTBEAT);
    }
}
