package com.databend.jdbc;

import com.vdurmont.semver4j.Semver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class Compatibility {
    public static class Capability {
        boolean streamingLoad;

        public Capability() {
            this.streamingLoad = true;
        }
        public Capability(boolean streamingLoad) {
            this.streamingLoad = streamingLoad;
        }

        public static Capability fromServerVersion(Semver ver) {
            boolean streamingLoad =  ver.isGreaterThan(new Semver("1.2.781"));
            return new Capability(streamingLoad);
        }

        public static Capability fromDriverVersion(Semver ver) {
            boolean streamingLoad =  ver.isGreaterThan(new Semver("0.4.1"));
            return new Capability(streamingLoad);
        }
    }

    static String port = System.getenv("DATABEND_TEST_CONN_PORT") != null ? System.getenv("DATABEND_TEST_CONN_PORT").trim() : "8000";
    public static Semver driverVersion = getDriverVersion();
    public static Semver serverVersion = getServerVersion();
    public static Capability driverCapability = driverVersion==null? new Capability(): Capability.fromDriverVersion(driverVersion);
    public static Capability serverCapability = serverVersion==null? new Capability(): Capability.fromServerVersion(serverVersion);

    private static Semver getDriverVersion() {
        String env = System.getenv("DATABEND_TEST_DRIVER_VERSION");
        if (env == null) {
            return null;
        }
        return new Semver(env);
    }
    private static Semver getServerVersion() {
        String env = System.getenv("DATABEND_TEST_SERVER_VERSION");
        if (env == null) {
            return null;
        }
        return new Semver(env);
    }

     public static boolean skipDriverBug(String version) {
        if (driverVersion != null && driverVersion.isLowerThan(new Semver(version))) {
           StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
           String callerName = stackTrace[2].getMethodName();
           System.out.println("SkipDriverBug version=" + version + ", method=" +  callerName);
           return true;
        }
        return false;
    }
}
