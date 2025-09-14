package com.databend.jdbc;

import com.databend.jdbc.log.DatabendLogger;
import com.databend.jdbc.log.JDKLogger;
import com.databend.jdbc.log.SLF4JLogger;
import lombok.CustomLog;
import lombok.experimental.UtilityClass;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.stream.Collectors;

@UtilityClass
@CustomLog
class LoggerUtil {

    private static Boolean slf4jAvailable;

    /**
     * Provides a {@link DatabendLogger} based on whether SLF4J is available or not.
     *
     * @param name logger name
     * @return a {@link DatabendLogger}
     */
    public static DatabendLogger getLogger(String name) {
        if (slf4jAvailable == null) {
            slf4jAvailable = isSlf4jJAvailable();
        }

        if (slf4jAvailable) {
            return new SLF4JLogger(name);
        } else {
            return new JDKLogger(name);
        }
    }

    /**
     * Logs the {@link InputStream}
     *
     * @param is the {@link InputStream}
     * @return a copy of the {@link InputStream} provided
     */
    public InputStream logInputStream(InputStream is) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            byte[] buffer = new byte[1024];
            int len;
            while ((len = is.read(buffer)) > -1) {
                baos.write(buffer, 0, len);
            }
            baos.flush();
            InputStream streamToLog = new ByteArrayInputStream(baos.toByteArray());
            String text = new BufferedReader(new InputStreamReader(streamToLog, StandardCharsets.UTF_8)).lines()
                    .collect(Collectors.joining("\n"));
            log.info("======================================");
            log.info(text);
            log.info("======================================");
            return new ByteArrayInputStream(baos.toByteArray());
        } catch (Exception ex) {
            log.warn("Could not log the stream", ex);
        }
        return new ByteArrayInputStream(baos.toByteArray());
    }

    private static boolean isSlf4jJAvailable() {
        try {
            Class.forName("org.slf4j.Logger");
            return true;
        } catch (ClassNotFoundException ex) {
            return false;
        }
    }
}
