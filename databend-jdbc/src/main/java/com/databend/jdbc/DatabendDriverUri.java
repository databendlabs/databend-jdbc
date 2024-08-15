package com.databend.jdbc;

import com.google.common.base.Splitter;
import com.google.common.collect.Maps;
import com.google.common.net.HostAndPort;
import okhttp3.OkHttpClient;

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.databend.client.OkHttpUtils.basicAuthInterceptor;
import static com.databend.client.OkHttpUtils.setupInsecureSsl;
import static com.databend.client.OkHttpUtils.tokenAuth;
import static com.databend.jdbc.ConnectionProperties.*;
import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Parses and extracts parameters from a databend JDBC URL
 */
public final class DatabendDriverUri {
    private static final Logger logger = Logger.getLogger(DatabendDriverUri.class.getPackage().getName());
    private static final String JDBC_URL_PREFIX = "jdbc:";
    private static final String JDBC_URL_START = JDBC_URL_PREFIX + "databend://";
    private static final Splitter QUERY_SPLITTER = Splitter.on('&').omitEmptyStrings();
    private static final Splitter ARG_SPLITTER = Splitter.on('=').limit(2);
    private static final int DEFAULT_HTTPS_PORT = 443;
    private static final int DEFAULT_HTTP_PORT = 8000;
    private final Properties properties;
    private final DatabendNodes nodes;
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

//    private final boolean useSecureConnection;

    private DatabendDriverUri(String url, Properties driverProperties)
            throws SQLException {
        Map.Entry<DatabendNodes, Map<String, String>> uriAndProperties = parse(url);
        List<URI> uris = uriAndProperties.getKey().getUris();
        this.properties = mergeProperties(uris.get(uris.size() - 1), uriAndProperties.getValue(), driverProperties);
        this.useSecureConnection = SSL.getValue(properties).orElse(false);
        this.useVerify = USE_VERIFY.getValue(properties).orElse(false);
        this.debug = DEBUG.getValue(properties).orElse(false);
        this.strNullAsNull = STRNULL_AS_NULL.getValue(properties).orElse(true);
        this.warehouse = WAREHOUSE.getValue(properties).orElse("");
        this.sslmode = SSL_MODE.getValue(properties).orElse("disable");
        this.tenant = TENANT.getValue(properties).orElse("");
        List<URI> finalUris = parseFinalUris(uris, this.useSecureConnection, this.sslmode);
        DatabendClientLoadBalancingPolicy policy = DatabendClientLoadBalancingPolicy.create(LOAD_BALANCING_POLICY.getValue(properties).orElse(DatabendClientLoadBalancingPolicy.DISABLED));
        DatabendNodes nodes = uriAndProperties.getKey();
        nodes.updateNodes(finalUris);
        nodes.updatePolicy(policy);
        this.nodes = nodes;
        System.out.println(nodes);
        this.database = DATABASE.getValue(properties).orElse("default");
        this.presignedUrlDisabled = PRESIGNED_URL_DISABLED.getRequiredValue(properties);
        this.copyPurge = COPY_PURGE.getValue(properties).orElse(true);
        this.nullDisplay = NULL_DISPLAY.getValue(properties).orElse("\\N");
        this.binaryFormat = BINARY_FORMAT.getValue(properties).orElse("");
        this.waitTimeSecs = WAIT_TIME_SECS.getRequiredValue(properties);
        this.connectionTimeout = CONNECTION_TIMEOUT.getRequiredValue(properties);
        this.queryTimeout = QUERY_TIMEOUT.getRequiredValue(properties);
        this.socketTimeout = SOCKET_TIMEOUT.getRequiredValue(properties);
        this.maxRowsInBuffer = ConnectionProperties.MAX_ROWS_IN_BUFFER.getRequiredValue(properties);
        this.maxRowsPerPage = ConnectionProperties.MAX_ROWS_PER_PAGE.getRequiredValue(properties);
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

    private static List<URI> parseFinalUris(List<URI> uris, boolean isSSLSecured, String sslmode) throws SQLException {
        List<URI> finalUris = new ArrayList<>();
        for (URI uri : uris) {
            finalUris.add(parseFinalURI(uri, isSSLSecured, sslmode));
        }
        // Sort the finalUris list
        finalUris.sort(new Comparator<URI>() {
            @Override
            public int compare(URI uri1, URI uri2) {
                // First, compare by host
                int hostComparison = uri1.getHost().compareTo(uri2.getHost());
                if (hostComparison != 0) {
                    return hostComparison;
                }

                // If hosts are the same, compare by port
                return Integer.compare(uri1.getPort(), uri2.getPort());
            }
        });

        return finalUris;
    }

    private static URI parseFinalURI(URI uri, boolean isSSLSecured, String sslmode) throws SQLException {
        requireNonNull(uri, "uri is null");
        String authority = uri.getAuthority();
        String scheme;
        if (isSSLSecured || sslmode.equals("enable")) {
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
    private static Map.Entry<DatabendNodes, Map<String, String>> parse(String url)
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
        String scheme;
        String host = null;
        int port = -1;
        raw = tryParseUriUserPassword(raw, uriProperties);

        // Split the URL into hosts and the rest
        // todo process bracket wrapped comma
        String[] hosts = raw.split(",");
        List<URI> uris = new ArrayList<>();
        try {
            for (String raw_host : hosts) {
                String fullUri = (raw_host.startsWith("http://") || raw_host.startsWith("https://")) ?
                        raw_host  :
                        "http://" + raw_host;

                URI uri = new URI(fullUri);
                String authority = uri.getAuthority();
                String[] hostAndPort = authority.split(":");
                if (hostAndPort.length == 2) {
                    host = hostAndPort[0];
                    port = Integer.parseInt(hostAndPort[1]);
                } else if (hostAndPort.length == 1) {
                    host = hostAndPort[0];
                } else {
                    throw new SQLException("Invalid host and port, url: " + url);
                }
                if (host == null || host.isEmpty()) {
                    throw new SQLException("Invalid host " + host);
                }
                // Set SSL properties based on the scheme
                if ("https".equals(uri.getScheme())) {
                    uriProperties.put(SSL.getKey(), "true");
                    uriProperties.put(SSL_MODE.getKey(), "enable");
                } else {
                    uriProperties.put(SSL.getKey(), "false");
                    uriProperties.put(SSL_MODE.getKey(), "disable");
                }
                uris.add(uri);
            }
            URI lastUri = uris.get(uris.size() - 1);
            // Initialize database from the last URI
            initDatabase(lastUri, uriProperties);
            // Sync path and query from lastUri for the rest of uri
            for (int i = 0; i < uris.size() - 1; i++) {
                URI uri = uris.get(i);
                uris.set(i, new URI(uri.getScheme(), uri.getUserInfo(), uri.getHost(), uri.getPort(), lastUri.getPath(), lastUri.getQuery(), lastUri.getFragment()));
            }
            // remove duplicate uris
            Set<URI> uriSet = new LinkedHashSet<>(uris);
            uris.clear();
            uris.addAll(uriSet);
            // Create DatabendNodes object
            DatabendClientLoadBalancingPolicy policy = DatabendClientLoadBalancingPolicy.create(DatabendClientLoadBalancingPolicy.DISABLED); // You might want to make this configurable
            DatabendNodes databendNodes = new DatabendNodes(uris, policy);
            return new AbstractMap.SimpleImmutableEntry<>(databendNodes, uriProperties);
        } catch (URISyntaxException e) {
            throw new SQLException("Invalid URI: " + raw, e);
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

    public DatabendNodes getNodes() {
        return nodes;
    }

    public URI getUri() {
        return nodes.getUris().get(0);
    }
    public URI getUri(String query_id) {
        return nodes.pickUri(query_id);
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

//    public HostAndPort getAddress() {
//        return address;
//    }

    public Properties getProperties() {
        return properties;
    }

    public void setupClient(OkHttpClient.Builder builder) throws SQLException {
        try {
            String password = PASSWORD.getValue(properties).orElse("");
            if (!password.isEmpty()) {
                builder.addInterceptor(basicAuthInterceptor(USER.getValue(properties).orElse(""), password));
            }
            if (useSecureConnection || sslmode.equals("enable")) {
                setupInsecureSsl(builder);
            }
            if (ACCESS_TOKEN.getValue(properties).isPresent()) {
                builder.addInterceptor(tokenAuth(ACCESS_TOKEN.getValue(properties).get()));
            }
            if (CONNECTION_TIMEOUT.getValue(properties).isPresent()) {
                builder.connectTimeout(CONNECTION_TIMEOUT.getValue(properties).get(), TimeUnit.SECONDS);
            }
            if (SOCKET_TIMEOUT.getValue(properties).isPresent()) {
                builder.readTimeout(SOCKET_TIMEOUT.getValue(properties).get(), TimeUnit.SECONDS);
            }

        } catch (Exception e) {
            throw new SQLException("Failed to setup client", e);
        }
    }

}
