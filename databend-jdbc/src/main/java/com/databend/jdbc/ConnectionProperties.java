package com.databend.jdbc;

import com.databend.client.PaginationOptions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.checkerframework.checker.units.qual.C;

import java.sql.Connection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

// all possible JDBC properties options currently supported by databend driver
public final class ConnectionProperties {
    public static final ConnectionProperty<String> USER = new User();
    public static final ConnectionProperty<String> PASSWORD = new Password();
    public static final ConnectionProperty<Boolean> SSL = new Ssl();

    public static final ConnectionProperty<Boolean> USE_VERIFY = new UseVerify();
    public static final ConnectionProperty<Boolean> STRNULL_AS_NULL = new StrNullAsNull();
    public static final ConnectionProperty<String> WAREHOUSE = new Warehouse();
    public static final ConnectionProperty<String> SSL_MODE = new SSLMode();
    static final ConnectionProperty<String> TENANT = new Tenant();
    public static final ConnectionProperty<String> DATABASE = new Database();
    public static final ConnectionProperty<String> ACCESS_TOKEN = new AccessToken();

    public static final ConnectionProperty<Integer> CONNECTION_TIMEOUT = new ConnectionTimeout();

    public static final ConnectionProperty<Integer> QUERY_TIMEOUT = new QueryTimeout();
    public static final ConnectionProperty<Integer> SOCKET_TIMEOUT = new SocketTimeout();

    public static final ConnectionProperty<Boolean> PRESIGNED_URL_DISABLED = new PresignedUrlDisabled();
    public static final ConnectionProperty<Boolean> COPY_PURGE = new CopyPurge();
    public static final ConnectionProperty<String> NULL_DISPLAY = new NullDisplay();
    public static final ConnectionProperty<String> BINARY_FORMAT = new BinaryFormat();
    public static final ConnectionProperty<Integer> WAIT_TIME_SECS = new WaitTimeSecs();

    public static final ConnectionProperty<Integer> MAX_ROWS_IN_BUFFER = new MaxRowsInBuffer();
    public static final ConnectionProperty<Integer> MAX_ROWS_PER_PAGE = new MaxRowsPerPage();

    private static final Set<ConnectionProperty<?>> ALL_PROPERTIES = ImmutableSet.<ConnectionProperty<?>>builder()
            .add(USER)
            .add(PASSWORD)
            .add(SSL)
            .add(USE_VERIFY)
            .add(STRNULL_AS_NULL)
            .add(WAREHOUSE)
            .add(SSL_MODE)
            .add(TENANT)
            .add(DATABASE)
            .add(ACCESS_TOKEN)
            .add(PRESIGNED_URL_DISABLED)
            .add(QUERY_TIMEOUT)
            .add(CONNECTION_TIMEOUT)
            .add(SOCKET_TIMEOUT)
            .add(WAIT_TIME_SECS)
            .add(MAX_ROWS_IN_BUFFER)
            .add(MAX_ROWS_PER_PAGE)
            .build();
    private static final Map<String, String> DEFAULTS;

    public static Set<ConnectionProperty<?>> allProperties() {
        return ALL_PROPERTIES;
    }

    public static Map<String, String> getDefaults() {
        return DEFAULTS;
    }

    private static class User
            extends AbstractConnectionProperty<String> {
        public User() {
            super("user", NOT_REQUIRED, ALLOWED, NON_EMPTY_STRING_CONVERTER);
        }
    }

    private static class Password
            extends AbstractConnectionProperty<String> {
        public Password() {
            super("password", NOT_REQUIRED, ALLOWED, STRING_CONVERTER);
        }
    }

    // whether enable ssl or not default is true
    private static class Ssl
            extends AbstractConnectionProperty<Boolean> {
        public Ssl() {
            super("ssl", Optional.of("false"), NOT_REQUIRED, ALLOWED, BOOLEAN_CONVERTER);
        }
    }

    private static class UseVerify
            extends AbstractConnectionProperty<Boolean> {
        public UseVerify() {
            super("use_verify", Optional.of("true"), NOT_REQUIRED, ALLOWED, BOOLEAN_CONVERTER);
        }
    }

    public static class StrNullAsNull
            extends AbstractConnectionProperty<Boolean> {
        public StrNullAsNull() {
            super("strnullasnull", Optional.of("true"), NOT_REQUIRED, ALLOWED, BOOLEAN_CONVERTER);
        }
    }

    private static class Database
            extends AbstractConnectionProperty<String> {
        public Database() {
            super("database", Optional.of("default"), NOT_REQUIRED, ALLOWED, STRING_CONVERTER);
        }
    }

    private static class Warehouse
            extends AbstractConnectionProperty<String> {
        public Warehouse() {
            super("warehouse", NOT_REQUIRED, ALLOWED, STRING_CONVERTER);
        }
    }

    private static class SSLMode extends AbstractConnectionProperty<String> {
        public SSLMode() {
            super("sslmode", Optional.of("disable"), NOT_REQUIRED, ALLOWED, STRING_CONVERTER);
        }
    }

    private static class Tenant
            extends AbstractConnectionProperty<String> {
        public Tenant() {
            super("tenant", NOT_REQUIRED, ALLOWED, STRING_CONVERTER);
        }
    }

    private static class AccessToken
            extends AbstractConnectionProperty<String> {
        public AccessToken() {
            super("accesstoken", NOT_REQUIRED, ALLOWED, STRING_CONVERTER);
        }
    }

    private static class PresignedUrlDisabled
            extends AbstractConnectionProperty<Boolean> {
        public PresignedUrlDisabled() {
            super("presigned_url_disabled", Optional.of("false"), NOT_REQUIRED, ALLOWED, BOOLEAN_CONVERTER);
        }
    }

    private static class CopyPurge extends AbstractConnectionProperty<Boolean> {
        public CopyPurge() {
            super("copy_purge", Optional.of("true"), NOT_REQUIRED, ALLOWED, BOOLEAN_CONVERTER);
        }
    }

    private static class NullDisplay
            extends AbstractConnectionProperty<String> {
        public NullDisplay() {
            super("null_display", Optional.of("\\N"), NOT_REQUIRED, ALLOWED, STRING_CONVERTER);
        }
    }

    private static class BinaryFormat
            extends AbstractConnectionProperty<String> {
        public BinaryFormat() {
            super("binary_format", Optional.of(""), NOT_REQUIRED, ALLOWED, STRING_CONVERTER);
        }
    }

    private static class QueryTimeout
            extends AbstractConnectionProperty<Integer> {
        public QueryTimeout() {
            super("query_timeout", Optional.of(String.valueOf(0)), NOT_REQUIRED, ALLOWED, INTEGER_CONVERTER);
        }
    }

    private static class ConnectionTimeout
            extends AbstractConnectionProperty<Integer> {
        public ConnectionTimeout() {
            super("connection_timeout", Optional.of(String.valueOf(0)), NOT_REQUIRED, ALLOWED, INTEGER_CONVERTER);
        }
    }

    private static class SocketTimeout extends AbstractConnectionProperty<Integer> {
        public SocketTimeout() {
            super("socket_timeout", Optional.of(String.valueOf(0)), NOT_REQUIRED, ALLOWED, INTEGER_CONVERTER);
        }
    }

    private static class WaitTimeSecs
            extends AbstractConnectionProperty<Integer> {
        public WaitTimeSecs() {
            super("wait_time_secs", Optional.of(String.valueOf(PaginationOptions.getDefaultWaitTimeSec())), NOT_REQUIRED, ALLOWED, INTEGER_CONVERTER);
        }
    }

    private static class MaxRowsInBuffer
            extends AbstractConnectionProperty<Integer> {
        public MaxRowsInBuffer() {
            super("max_rows_in_buffer", Optional.of(String.valueOf(PaginationOptions.getDefaultMaxRowsInBuffer())), NOT_REQUIRED, ALLOWED, INTEGER_CONVERTER);
        }
    }

    private static class MaxRowsPerPage
            extends AbstractConnectionProperty<Integer> {
        public MaxRowsPerPage() {
            super("max_rows_per_page", Optional.of(String.valueOf(PaginationOptions.getDefaultMaxRowsPerPage())), NOT_REQUIRED, ALLOWED, INTEGER_CONVERTER);
        }
    }

    static {
        ImmutableMap.Builder<String, String> defaults = ImmutableMap.builder();
        for (ConnectionProperty<?> property : ALL_PROPERTIES) {
            property.getDefault().ifPresent(value -> defaults.put(property.getKey(), value));
        }
        DEFAULTS = defaults.buildOrThrow();
    }
}
