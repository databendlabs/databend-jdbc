package com.databend.jdbc.examples;

import com.databend.jdbc.DatabendConnectionImpl;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class DatabendConnectionFactory implements PooledObjectFactory<DatabendConnectionImpl> {

    private String url;
    private Properties properties;

    public DatabendConnectionFactory(String url, Properties properties) {
        this.url = url;
        this.properties = properties;
    }

    private Connection createConnection(String url, Properties p) throws SQLException {
        return DriverManager.getConnection(url, p);
    }

    @Override
    public PooledObject<DatabendConnectionImpl> makeObject() throws Exception {
        DatabendConnectionImpl connection = (DatabendConnectionImpl) createConnection(url, properties);
        return new DefaultPooledObject<>(connection);
    }

    @Override
    public void destroyObject(PooledObject<DatabendConnectionImpl> p) throws Exception {
        p.getObject().close();
    }

    @Override
    public boolean validateObject(PooledObject<DatabendConnectionImpl> p) {
        try {
            return !p.getObject().isClosed();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void activateObject(PooledObject<DatabendConnectionImpl> p) throws Exception {
    }

    @Override
    public void passivateObject(PooledObject<DatabendConnectionImpl> p) throws Exception {
    }
}

