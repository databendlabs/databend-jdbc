package com.databend.jdbc.examples;

import com.databend.jdbc.DatabendConnection;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import java.util.Properties;

public class DatabendConnectionPool extends GenericObjectPool<DatabendConnection> {
    public DatabendConnectionPool(DatabendConnectionFactory factory, GenericObjectPoolConfig<DatabendConnection> config) {
        super(factory);
    }

    public void testDemo() throws Exception {
        GenericObjectPoolConfig<DatabendConnection> config = new GenericObjectPoolConfig<>();
        config.setMaxTotal(10); // 设置最大连接数
        config.setMinIdle(2); // 设置最小空闲连接数

        Properties props = new Properties();
        props.setProperty("database", "db3");
        props.setProperty("SSL", "false");
        // Create a Databend connection pool
        DatabendConnectionFactory factory = new DatabendConnectionFactory("jdbc:databend://localhost:8000", props);
        DatabendConnectionPool pool = new DatabendConnectionPool(factory,config);

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


