package com.databend.jdbc.examples;

import com.databend.jdbc.DatabendConnection;
import org.apache.commons.pool2.impl.GenericObjectPool;

import java.util.Properties;

public class DatabendConnectionPool extends GenericObjectPool<DatabendConnection> {
    public DatabendConnectionPool(DatabendConnectionFactory factory) {
        super(factory);
    }

    public void testDemo() throws Exception {
        Properties props = new Properties();
        props.setProperty("database", "db3");
        props.setProperty("SSL", "false");
        // Create a Databend connection pool
        DatabendConnectionFactory factory = new DatabendConnectionFactory("jdbc:databend://localhost:8000", props);
        DatabendConnectionPool pool = new DatabendConnectionPool(factory);

        // Get a connection from the pool
        DatabendConnection connection = pool.borrowObject();
//        connection.uploadStream();

        // Use the connection
        connection.createStatement().executeQuery("SELECT version()");

        // Return the connection to the pool
        pool.returnObject(connection);

        // Close the pool when done
        pool.close();
    }
}


