package com.databend.jdbc.examples;

import com.databend.jdbc.DatabendConnectionImpl;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import java.util.Properties;

public class DatabendConnectionPool extends GenericObjectPool<DatabendConnectionImpl> {
    public DatabendConnectionPool(DatabendConnectionFactory factory, GenericObjectPoolConfig<DatabendConnectionImpl> config) {
        super(factory, config);
    }

    public void testDemo() throws Exception {
        GenericObjectPoolConfig<DatabendConnectionImpl> config = new GenericObjectPoolConfig<>();
        // set max total connection
        config.setMaxTotal(10);
        // set min idle connection
        config.setMinIdle(2);

        Properties props = new Properties();
        props.setProperty("database", "db3");
        props.setProperty("SSL", "false");
        // Create a Databend connection pool
        DatabendConnectionFactory factory = new DatabendConnectionFactory("jdbc:databend://localhost:8000", props);
        DatabendConnectionPool pool = new DatabendConnectionPool(factory, config);

        // Get a connection from the pool
        DatabendConnectionImpl connection = pool.borrowObject();
//        connection.uploadStream();

        // Use the connection
        connection.createStatement().executeQuery("SELECT version()");

        // Return the connection to the pool
        pool.returnObject(connection);

        // Close the pool when done
        pool.close();
    }
}


