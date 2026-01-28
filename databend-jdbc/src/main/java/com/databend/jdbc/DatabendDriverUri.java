package com.databend.jdbc;

import com.databend.jdbc.util.GlobalCookieJar;
import com.databend.jdbc.util.URLUtils;
import com.google.common.base.Splitter;
import com.google.common.collect.Maps;
import com.google.common.net.HostAndPort;
import okhttp3.Cookie;
import okhttp3.OkHttpClient;

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import static com.databend.client.OkHttpUtils.*;
import static com.databend.jdbc.ConnectionProperties.*;
import static com.databend.jdbc.DatabendConstant.ENABLE_STR;
import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Parses and extracts parameters from a databend JDBC URL
 */
final class DatabendDriverUri {
    private static final String JDBC_URL_PREFIX = "jdbc:";
    private static final String JDBC_URL_START = JDBC_URL_PREFIX + "databend://";
    private static final Splitter QUERY_SPLITTER = Splitter.on('&').omitEmptyStrings();
    private static final Splitter ARG_SPLITTER = Splitter.on('=').limit(2);
    private static final int DEFAULT_HTTPS_PORT = 443;
    private static final int DEFAULT_HTTP_PORT = 8000;
    private static final Logger logger = Logger.getLogger(DatabendDriverUri.class.getPackage().getName());
    private final Properties properties;
    private final URI uri;
    private final boolean useSecureConnection;
    private final boolean useVerify;
    private final boolean debug;
    private final boolean strNullAsNull;
    private final String warehouse;
    private final String sslmode;
    private final String tenant;
    private final boolean copyPurge;
    private final String nullDisplay;
    private final String binaryFormat;
    private final String database;
    private final boolean presignedUrlDisabled;
    private final Integer connectionTimeout;
    private final Integer queryTimeout;
    private final Integer socketTimeout;
    private final Integer waitTimeSecs;
    private final Integer maxRowsInBuffer;
    private final Integer maxRowsPerPage;

    private final Map<String, String> sessionSettings;

//    private final boolean useSecureConnection;

    private DatabendDriverUri(String url, Properties driverProperties)
            throws SQLException {
        Map.Entry<URI, Map<String, String>> uriAndProperties = parse(url);
        URI rawUri = uriAndProperties.getKey();
        this.properties = mergeProperties(rawUri, uriAndProperties.getValue(), driverProperties);
        this.useSecureConnection = SSL.getValue(properties).orElse(false);
        this.useVerify = USE_VERIFY.getValue(properties).orElse(false);
        this.debug = DEBUG.getValue(properties).orElse(false);
        this.strNullAsNull = STRNULL_AS_NULL.getValue(properties).orElse(true);
        this.warehouse = WAREHOUSE.getValue(properties).orElse("");
        this.sslmode = SSL_MODE.getValue(properties).orElse("disable");
        this.tenant = TENANT.getValue(properties).orElse("");
        this.uri = canonicalizeUri(rawUri, this.useSecureConnection, this.sslmode);
        this.database = DATABASE.getValue(properties).orElse("default");
        this.presignedUrlDisabled = PRESIGNED_URL_DISABLED.getRequiredValue(properties);
        this.copyPurge = COPY_PURGE.getValue(properties).orElse(true);
        this.nullDisplay = NULL_DISPLAY.getValue(properties).orElse("\\N");
        this.binaryFormat = BINARY_FORMAT.getValue(properties).orElse("");
        this.waitTimeSecs = WAIT_TIME_SECS.getRequiredValue(properties);
        this.connectionTimeout = CONNECTION_TIMEOUT.getRequiredValue(properties);
        this.queryTimeout = QUERY_TIMEOUT.getRequiredValue(properties);
        this.maxRowsInBuffer = ConnectionProperties.MAX_ROWS_IN_BUFFER.getRequiredValue(properties);
        this.maxRowsPerPage = ConnectionProperties.MAX_ROWS_PER_PAGE.getRequiredValue(properties);
        Integer socketTimeout = SOCKET_TIMEOUT.getRequiredValue(properties);
        if (socketTimeout <= this.waitTimeSecs + 10) {
            this.socketTimeout = this.waitTimeSecs + 10;
        } else {
            this.socketTimeout = socketTimeout;
        }

        String settingsStr = SESSION_SETTINGS.getValue(properties).orElse("");
        this.sessionSettings = parseSessionSettings(settingsStr);

        warnDeprecatedMultiHostProperties(this.properties);

    }

    private Map<String, String> parseSessionSettings(String settingsStr) {
        if (isNullOrEmpty(settingsStr)) {
            return new HashMap<>();
        }

        // key1=value1,key2=value2
        Map<String, String> settings = new HashMap<>();
        String[] pairs = settingsStr.split(",");
        for (String pair : pairs) {
            String[] keyValue = pair.split("=", 2);
            if (keyValue.length == 2) {
                settings.put(keyValue[0].trim(), keyValue[1].trim());
            }
        }
        return settings;
    }


    public static DatabendDriverUri create(String url, Properties properties)
            throws SQLException {
        return new DatabendDriverUri(url, firstNonNull(properties, new Properties()));
    }

    private static void initDatabase(URI uri, Map<String, String> uriProperties) throws SQLException {
        String path = uri.getPath();
        if (isNullOrEmpty(path) || "/".equals(path)) {
            return;
        }
        if (!path.startsWith("/")) {
            throw new SQLException(format("Invalid database name '%s'", path));
        }

        String db = path.substring(1);
        uriProperties.put(DATABASE.getKey(), db);
    }

    private static URI canonicalizeUri(URI uri, boolean isSSLSecured, String sslmode) throws SQLException {
        requireNonNull(uri, "uri is null");
        String authority = uri.getAuthority();
        String scheme;
        if (isSSLSecured || "enable".equals(sslmode)) {
            scheme = "https";
        } else {
            scheme = "http";
        }
        int finalPort = -1;
        try {
            HostAndPort hostAndPort = HostAndPort.fromString(authority);
            if (hostAndPort.hasPort()) {
                finalPort = hostAndPort.getPort();
                return new URI(scheme, uri.getUserInfo(), uri.getHost(), finalPort, uri.getPath(), uri.getQuery(), uri.getFragment());
            }
        } catch (Exception e) {
            // ignore
        }
        if (finalPort == -1) {
            finalPort = isSSLSecured ? DEFAULT_HTTPS_PORT : DEFAULT_HTTP_PORT;
        }
        try {

            return new URI(scheme, uri.getUserInfo(), uri.getHost(), finalPort, uri.getPath(), uri.getQuery(), uri.getFragment());
        } catch (URISyntaxException e) {
            throw new SQLException("Invalid URI: " + uri, e);
        }
    }

    private static String tryParseUriUserPassword(String url, Map<String, String> properties) {
        int atPos = url.lastIndexOf('@');
        if (atPos > 0) {
            String userPass = url.substring(0, atPos);
            int colonPos = userPass.indexOf(':');
            if (colonPos > 0) {
                String user = userPass.substring(0, colonPos);
                String pass = userPass.substring(colonPos + 1);
                String encodePass = URLUtils.urlEncode(pass);
                properties.put(USER.getKey(), user);
                properties.put(ConnectionProperties.PASSWORD.getKey(), encodePass);
            } else {
                properties.put(USER.getKey(), userPass);
            }
            url = url.substring(atPos + 1);
        }
        return url;
    }

    private static void setProperties(Properties properties, Map<String, String> values) {
        for (Map.Entry<String, String> entry : values.entrySet()) {
            properties.setProperty(entry.getKey().toLowerCase(Locale.US), entry.getValue());
        }
    }

    private static Properties mergeProperties(URI uri, Map<String, String> uriProperties, Properties driverProperties) throws SQLException {
        Map<String, String> defaults = ConnectionProperties.getDefaults();
        Map<String, String> urlProperties = parseParameters(uri.getQuery());
        Map<String, String> suppliedProperties = Maps.fromProperties(driverProperties);
        Properties result = new Properties();
        setProperties(result, defaults);
        setProperties(result, uriProperties);
        setProperties(result, urlProperties);
        setProperties(result, suppliedProperties);
        return result;
    }

    // needs to parse possible host, port, username, password, tenant, warehouse, database, etc.
    private static Map.Entry<URI, Map<String, String>> parse(String url)
            throws SQLException {
        if (url == null) {
            throw new SQLException("URL is null");
        }

        int pos = url.indexOf(JDBC_URL_START);
        if (pos != 0) {
            throw new SQLException("Invalid JDBC URL: " + url + " URL does not start with " + JDBC_URL_START);
        }
        Map<String, String> uriProperties = new LinkedHashMap<>();
        String raw = url.substring(pos + JDBC_URL_START.length());
        raw = tryParseUriUserPassword(raw, uriProperties);
        ensureSingleHostAuthority(raw, url);

        try {
            URI uri = parseSingleUri(raw);
            if (uri.getHost() == null || uri.getHost().isEmpty()) {
                throw new SQLException("Invalid host " + uri.getHost());
            }

            if ("https".equals(uri.getScheme())) {
                uriProperties.put(SSL.getKey(), "true");
                uriProperties.put(SSL_MODE.getKey(), "enable");
            } else {
                uriProperties.put(SSL.getKey(), "false");
                uriProperties.put(SSL_MODE.getKey(), "disable");
            }

            initDatabase(uri, uriProperties);
            return new AbstractMap.SimpleImmutableEntry<>(uri, uriProperties);
        } catch (URISyntaxException e) {
            throw new SQLException("Invalid URI: " + raw, e);
        }
    }

    private static URI parseSingleUri(String rawHost) throws URISyntaxException {
        String fullUri = (rawHost.startsWith("http://") || rawHost.startsWith("https://")) ?
                rawHost :
                "http://" + rawHost;
        return new URI(fullUri);
    }

    private static void ensureSingleHostAuthority(String raw, String originalUrl) throws SQLException {
        int endOfAuthority = raw.length();
        int slashIndex = raw.indexOf('/');
        if (slashIndex != -1 && slashIndex < endOfAuthority) {
            endOfAuthority = slashIndex;
        }
        int questionIndex = raw.indexOf('?');
        if (questionIndex != -1 && questionIndex < endOfAuthority) {
            endOfAuthority = questionIndex;
        }
        int fragmentIndex = raw.indexOf('#');
        if (fragmentIndex != -1 && fragmentIndex < endOfAuthority) {
            endOfAuthority = fragmentIndex;
        }
        String authoritySection = raw.substring(0, endOfAuthority);
        if (authoritySection.contains(",")) {
            throw new SQLException("Multiple hosts in JDBC URL are not supported: " + originalUrl);
        }
    }

    public static boolean acceptsURL(String url) {
        return url.startsWith(JDBC_URL_START);
    }

    private static Map<String, String> parseParameters(String query)
            throws SQLException {
        Map<String, String> result = new HashMap<>();

        if (query != null) {
            Iterable<String> queryArgs = QUERY_SPLITTER.split(query);
            for (String queryArg : queryArgs) {
                List<String> parts = ARG_SPLITTER.splitToList(queryArg);
                if (parts.size() != 2) {
                    throw new SQLException(format("Connection argument is not valid connection property: '%s'", queryArg));
                }
                if (result.put(parts.get(0), parts.get(1)) != null) {
                    throw new SQLException(format("Connection property '%s' is in URL multiple times", parts.get(0)));
                }
            }
        }

        return result;
    }

    public URI getUri() {
        return uri;
    }

    public URI getUri(String query_id) {
        return getUri();
    }

    public String getDatabase() {
        return database;
    }

    public Boolean presignedUrlDisabled() {
        return presignedUrlDisabled;
    }

    public Boolean copyPurge() {
        return copyPurge;
    }

    public String getWarehouse() {
        return warehouse;
    }

    public boolean getStrNullAsNull() {
        return strNullAsNull;
    }

    public boolean getUseVerify() {
        return useVerify;
    }

    public boolean getDebug() {
        return debug;
    }

    public String getSslmode() {
        return sslmode;
    }

    public String getTenant() {
        return tenant;
    }

    public String nullDisplay() {
        return nullDisplay;
    }

    public String binaryFormat() {
        return binaryFormat;
    }

    public Integer getConnectionTimeout() {
        return connectionTimeout;
    }

    public Integer getQueryTimeout() {
        return queryTimeout;
    }

    public Integer getSocketTimeout() {
        return socketTimeout;
    }

    public Integer getWaitTimeSecs() {
        return waitTimeSecs;
    }

    public Integer getMaxRowsInBuffer() {
        return maxRowsInBuffer;
    }

    public Integer getMaxRowsPerPage() {
        return maxRowsPerPage;
    }

    public Map<String, String> getSessionSettings() {
        return sessionSettings;
    }

    public Properties getProperties() {
        return properties;
    }

    public void setupClient(OkHttpClient.Builder builder) throws SQLException {
        try {
            GlobalCookieJar cookieJar = new GlobalCookieJar();
            cookieJar.add(new Cookie.Builder().name("cookie_enabled").value("true").domain("not_used").build());
            builder.cookieJar(cookieJar);

            String password = PASSWORD.getValue(properties).orElse("");
            builder.addInterceptor(basicAuthInterceptor(USER.getValue(properties).orElse(""), password));
            if (useSecureConnection || ENABLE_STR.equals(sslmode)) {
                setupInsecureSsl(builder);
            }
            if (ACCESS_TOKEN.getValue(properties).isPresent()) {
                builder.addInterceptor(tokenAuth(ACCESS_TOKEN.getValue(properties).get()));
            }
            if (CONNECTION_TIMEOUT.getValue(properties).isPresent()) {
                Integer timeout =  CONNECTION_TIMEOUT.getValue(properties).get();
                if (timeout != 0)
                    builder.connectTimeout(timeout, TimeUnit.SECONDS);
            }
            if (SOCKET_TIMEOUT.getValue(properties).isPresent()) {
                builder.readTimeout(SOCKET_TIMEOUT.getValue(properties).get(), TimeUnit.SECONDS);
            }

        } catch (Exception e) {
            throw new SQLException("Failed to setup client", e);
        }
    }

    private void warnDeprecatedMultiHostProperties(Properties mergedProperties) {
        warnDeprecatedProperty(mergedProperties, LOAD_BALANCING_POLICY);
        warnDeprecatedProperty(mergedProperties, MAX_FAILOVER_RETRY);
        warnDeprecatedProperty(mergedProperties, AUTO_DISCOVERY);
        warnDeprecatedProperty(mergedProperties, NODE_DISCOVERY_INTERVAL);
        warnDeprecatedProperty(mergedProperties, ENABLE_MOCK);
    }

    private void warnDeprecatedProperty(Properties mergedProperties, ConnectionProperty<?> property) {
        if (mergedProperties.containsKey(property.getKey())) {
            logger.warning("Connection property '" + property.getKey() + "' is deprecated and ignored because multi-host features were removed.");
        }
    }
}
