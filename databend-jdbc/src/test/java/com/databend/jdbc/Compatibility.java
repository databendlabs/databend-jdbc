package com.databend.jdbc;

import com.vdurmont.semver4j.Semver;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;

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
            boolean streamingLoad =  ver.isGreaterThanOrEqualTo(new Semver("1.2.792"));
            return new Capability(streamingLoad);
        }

        public static Capability fromDriverVersion(Semver ver) {
            boolean streamingLoad =  ver.isGreaterThanOrEqualTo(new Semver("0.4.1"));
            return new Capability(streamingLoad);
        }
    }

    public static Semver driverVersion = getDriverVersion();
    public static Semver serverVersion = getServerVersion();
    public static Capability driverCapability = driverVersion==null? new Capability(): Capability.fromDriverVersion(driverVersion);
    public static Capability serverCapability = serverVersion==null? new Capability(): Capability.fromServerVersion(serverVersion);

    private static Semver getDriverVersion() {
        String env = System.getenv("DATABEND_JDBC_VERSION");
        if (env == null) {
            return null;
        }
        return new Semver(env);
    }
    private static Semver getServerVersion() {
        String env = System.getenv("DATABEND_QUERY_VERSION");
        if (env == null || "nightly".equals(env)) {
            return null;
        }
        return new Semver(env, Semver.SemverType.NPM).withClearedSuffixAndBuild();
    }

     public static boolean skipDriverBugLowerThen(String version) {
        if (driverVersion != null && driverVersion.isLowerThan(new Semver(version))) {
           StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
           String callerName = stackTrace[2].getMethodName();
           System.out.println("SkipDriverBug version=" + version + ", method=" +  callerName);
           return true;
        }
        return false;
    }
    public static boolean skipBugLowerThenOrEqualTo(String serverVersionBug, String driverVersionBug) {
        if (driverVersion != null && driverVersion.isLowerThanOrEqualTo(new Semver(serverVersionBug))
                && serverVersion != null && serverVersion.isLowerThanOrEqualTo(serverVersionBug)
        ) {
            StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
            String callerName = stackTrace[2].getMethodName();
            System.out.printf("SkipDriverBug (server <= %s && driver <=%s), method = %s",
                    serverVersionBug, driverVersionBug, callerName);
            return true;
        }
        return false;
    }

    boolean isNewInterface() {
        return driverVersion == null || driverVersion.isGreaterThanOrEqualTo(new Semver("4.0.1"));
    }

    static Object invokeMethod(Object target, String methodName,
                                      Class<?>[] parameterTypes, Object[] args) {
        Class<?> targetClass = target.getClass();
        Method method = null;
        try {
            method = targetClass.getDeclaredMethod(methodName, parameterTypes);
            method.setAccessible(true);
            return method.invoke(target, args);
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    static Object invokeMethodNoArg(Object target, String methodName) {
        return invokeMethod(target, methodName, new Class<?>[0], new Object[0]);
    }
}
