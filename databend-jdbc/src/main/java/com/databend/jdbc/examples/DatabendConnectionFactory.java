package com.databend.jdbc.examples;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class DatabendConnectionFactory implements PooledObjectFactory<Connection> {

    final private String url;
    final private Properties properties;

    public DatabendConnectionFactory(String url, Properties properties) {
        this.url = url;
        this.properties = properties;
    }

    private Connection createConnection(String url, Properties p) throws SQLException {
        return DriverManager.getConnection(url, p);
    }

    @Override
    public PooledObject<Connection> makeObject() throws Exception {
        Connection connection = createConnection(url, properties);
        return new DefaultPooledObject<>(connection);
    }

    @Override
    public void destroyObject(PooledObject<Connection> p) throws Exception {
        p.getObject().close();
    }

    @Override
    public boolean validateObject(PooledObject<Connection> p) {
        try {
            return !p.getObject().isClosed();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void activateObject(PooledObject<Connection> p) throws Exception {
    }

    @Override
    public void passivateObject(PooledObject<Connection> p) throws Exception {
    }
}

