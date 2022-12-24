package com.databend.jdbc;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

// all possible JDBC properties options currently supported by databend driver
final class ConnectionProperties
{
    public static final ConnectionProperty<String> USER = new User();
    public static final ConnectionProperty<String> PASSWORD = new Password();
    public static final ConnectionProperty<Boolean> SSL = new Ssl();
    public static final ConnectionProperty<String> DATABASE = new Database();
    public static final ConnectionProperty<String> ACCESS_TOKEN = new AccessToken();

    private static final Set<ConnectionProperty<?>> ALL_PROPERTIES = ImmutableSet.<ConnectionProperty<?>>builder()
            .add(USER)
            .add(PASSWORD)
            .add(SSL)
            .add(DATABASE)
            .add(ACCESS_TOKEN)
            .build();
    private static final Map<String, String> DEFAULTS;

    public static Set<ConnectionProperty<?>> allProperties()
    {
        return ALL_PROPERTIES;
    }

    public static Map<String, String> getDefaults()
    {
        return DEFAULTS;
    }

    private static class User
            extends AbstractConnectionProperty<String>
    {
        public User()
        {
            super("user", NOT_REQUIRED, ALLOWED, NON_EMPTY_STRING_CONVERTER);
        }
    }

    private static class Password
            extends AbstractConnectionProperty<String>
    {
        public Password()
        {
            super("password", NOT_REQUIRED, ALLOWED, STRING_CONVERTER);
        }
    }

    // whether enable ssl or not default is true
    private static class Ssl
            extends AbstractConnectionProperty<Boolean>
    {
        public Ssl()
        {
            super("ssl", Optional.of("false"), NOT_REQUIRED, ALLOWED, BOOLEAN_CONVERTER);
        }
    }

    private static class Database
            extends AbstractConnectionProperty<String>
    {
        public Database()
        {
            super("database", Optional.of("default"),  NOT_REQUIRED, ALLOWED, STRING_CONVERTER);
        }
    }

    private static class AccessToken
            extends AbstractConnectionProperty<String>
    {
        public AccessToken()
        {
            super("accesstoken", NOT_REQUIRED, ALLOWED, STRING_CONVERTER);
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
