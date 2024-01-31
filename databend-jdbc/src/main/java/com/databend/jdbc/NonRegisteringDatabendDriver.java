package com.databend.jdbc;

import okhttp3.OkHttpClient;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

import static com.databend.client.OkHttpUtils.userAgentInterceptor;
import static com.databend.jdbc.DriverInfo.DRIVER_NAME;
import static com.databend.jdbc.DriverInfo.DRIVER_VERSION;
import static com.databend.jdbc.DriverInfo.DRIVER_VERSION_MAJOR;
import static com.databend.jdbc.DriverInfo.DRIVER_VERSION_MINOR;

public class NonRegisteringDatabendDriver implements Driver, Closeable
{
    private final OkHttpClient httpClient = newHttpClient();

    private static Properties urlProperties(String url, Properties info)
    {
        try {
            return DatabendDriverUri.create(url, info).getProperties();
        }
        catch (SQLException e) {
            return info;
        }
    }

    private static OkHttpClient newHttpClient()
    {
        OkHttpClient.Builder builder = new OkHttpClient.Builder()
                .addInterceptor(userAgentInterceptor(DRIVER_NAME + "/" + DRIVER_VERSION));
        return builder.build();
    }

    @Override
    public void close()
    {
        httpClient.dispatcher().executorService().shutdown();
        httpClient.connectionPool().evictAll();
    }

    @Override
    public boolean acceptsURL(String url)
            throws SQLException
    {
        if (url == null) {
            throw new SQLException("URL is null");
        }
        return DatabendDriverUri.acceptsURL(url);
    }

    @Override
    public Connection connect(String url, Properties info)
            throws SQLException
    {
        if (!acceptsURL(url)) {
            return null;
        }

        DatabendDriverUri uri = DatabendDriverUri.create(url, info);

        OkHttpClient.Builder builder = httpClient.newBuilder();
        uri.setupClient(builder);
        DatabendConnection connection = new DatabendConnection(uri, builder.build());
        // ping the server host
        try {
            connection.PingDatabendClientV1();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return new DatabendConnection(uri, builder.build());
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info)
            throws SQLException
    {
        Properties properties = urlProperties(url, info);

        return ConnectionProperties.allProperties().stream()
                .filter(property -> property.isAllowed(properties))
                .map(property -> property.getDriverPropertyInfo(properties))
                .toArray(DriverPropertyInfo[]::new);
    }

    @Override
    public int getMajorVersion()
    {
        return DRIVER_VERSION_MAJOR;
    }

    @Override
    public int getMinorVersion()
    {
        return DRIVER_VERSION_MINOR;
    }

    @Override
    public boolean jdbcCompliant()
    {
        // TODO: pass compliance tests
        return false;
    }

    @Override
    public Logger getParentLogger()
            throws SQLFeatureNotSupportedException
    {
        // TODO: support java.util.Logging
        throw new SQLFeatureNotSupportedException();
    }


}
