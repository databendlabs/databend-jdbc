# JDBC Driver

Databend JDBC driver access databend distributions or databend cloud
through [REST API]{https://databend.rs/doc/integrations/api/rest}.
To use jdbc documentation, you could add the following dependency from maven central

```xml

<dependency>
    <groupId>com.databend</groupId>
    <artifactId>databend-jdbc</artifactId>
    <version>|version|</version>
</dependency>
```

## Driver Name

The driver class name is `com.databend.jdbc.DatabendDriver`. Most JDBC drivers are automatically loaded by
the `DriverManager` class. However, if you are using a JDBC driver that is not automatically loaded, you can load it by
calling the `Class.forName` method.

## Connecting

The following JDBC URL formats are supported

```text
jdbc:databend://host:port
jdbc:databend://user:password@host:port
jdbc:databend://host:port/database
jdbc:databend://user:password@host:port/database
```

For example, the following URL connects to databend host on your local machine with host `0.0.0.0` port `8000` and
database `hello_databend`
with username `databend` password `secret`

```text
jdbc:databend://databend:secret@0.0.0.0:8000/hello_databend
```

The above URL can be used as follows

```java 
String url="jdbc:databend://databend:secret@0.0.0.0:8000/hello_databend"
        Connection conn=DriverManager.getConnection(url);
```

If you are using [Databend Cloud](https://app.databend.com/), you can get a warehouse DSN according
to [this doc](https://databend.rs/cloud/using-databend-cloud/warehouses#connecting).
Then the above URL within warehouse DSN can be used as follows:

```java 
        String url="jdbc:databend://cloudapp:password@tnf34b0rm--elt-wh-medium.gw.aliyun-cn-beijing.default.databend.cn:443/db_name?ssl=true"
        Connection conn=DriverManager.getConnection(url);
```

### Configure load balancing and failover

Load balancing in Databend JDBC works by routing queries to different endpoints specified in the JDBC URL based on the chosen policy. This allows for better distribution of workload across multiple Databend nodes.

#### Load Balancing Options

There are three load balancing options available:

1. **disabled**: Only routes queries to the first endpoint provided (sorted in alphabetic order).
2. **random**: Randomly distributes queries based on the query ID.
3. **round_robin**: Distributes queries evenly to each node in a circular order.

#### Configurable Parameters

| Parameter | Description | Default | Example |
|-----------|-------------|---------|---------|
| load_balancing_policy | Specifies the load balancing policy for multi-host connections. Options are "disabled", "random", and "round_robin". | disabled | jdbc:databend://localhost:8000,localhost:8002,localhost:8003/default?load_balancing_policy=random |
| max_failover_retry | Specifies the maximum number of retry attempts for failover connections. | 0 | jdbc:databend://localhost:7222,localhost:7223,localhost:7224,localhost:8000/default?max_failover_retry=4 |

#### Examples

1. Basic load balancing with round-robin policy: `jdbc:databend://localhost:8000,localhost:8002,localhost:8003/default?load_balancing_policy=round_robin`
2. Load balancing with random policy and failover configuration:: `jdbc:databend://localhost:8000,localhost:8002,localhost:8003/default?load_balancing_policy=random&max_failover_retry=3
`
3. Load balancing with SSL enabled:`jdbc:databend://localhost:8000,localhost:8002,localhost:8003/default?ssl=true&load_balancing_policy=round_robin`

**NOTICE:**

1. When configuring SSL, it's recommended to use the approach shown in the last example, which allows for more detailed SSL configuration including certificate verification.

2. Remember to replace the hostnames, ports, and file paths with your actual Databend cluster configuration and SSL certificate locations.

3. Failover retry occur only for connection issues (java.net.ConnectException), other exception will NOT trigger retry.
4. Databend-jdbc support Transaction. During a transaction, the connection will be pinned to the same node, and the load balancing policy will be disabled. once the transaction is commited or aborted the connection will be released and the load balancing policy will be enabled again.




#### Automatic Node Discovery

| Parameter              | Description                                                                                                               | Default       | example                                                                                 |                                                             
|------------------------|---------------------------------------------------------------------------------------------------------------------------|---------------|-----------------------------------------------------------------------------------------|
| auto_discovery | Automatically discover possible cluster nodes in a databend query cluster | false         | jdbc:databend://0.0.0.0:8000/default?auto_discovery=true                                |
| node_discovery_interval | Minimum interval between two automatic node discovery actions in milliseconds | 5 * 60 * 1000 | jdbc:databend://0.0.0.0:8000/default?auto_discovery=true&node_discovery_interval=600000 |

Automatic Node Discovery will try to discover existing databend query cluster using /v1/discovery_nodes api, it will be closed if the target api is not supported on your databend version(minimum version:  v1.2.629-nightly), it passsively probe the possible node list which new query occured after given `node_discovery_interval` and update possible node  lists used for load balancing and failover. it will not use thread pool or executor service to start a background thread to handle the task.

**NOTICE:**
As the cluster ip/dns may vary based your network environment, it is recommend to give all possible nodes in the same warehouse and tenant a fixed ip or dns for reliable node discovery.
Sample Configuration:

```toml
[query]
discovery_address = "localhost:8000"

# Databend Query HTTP Handler.
http_handler_host = "0.0.0.0"
http_handler_port = 8000

tenant_id = "test_tenant"
cluster_id = "test_cluster"
```


In the above node configuration file, `discovery_address` is used for jdbc to connect the target node if it was discovered by the node discovery api from other nodes located in the same warehouse(with same tenant_id and cluster_id)
If `discovery_address` is not set, the address is determined based on three scenarios:
1. If the user has directly modified the discovery-address in the configuration, this value is returned.
2. If the user has configured an HTTP address that is not 0.0.0.0 or 127.0.0.1, this HTTP address is returned.
3. If the user has configured an HTTP address as 0.0.0.0 or 127.0.0.1, the system will probe to detect a suitable IP address. The IP address that is successfully routed through the network and can communicate with the meta service will be returned.


## Connection parameters

The driver supports various parameters that may be set as URL parameters or as properties passed to DriverManager. Both
of the following examples are equivalent:

```java
// URL parameters
String url="jdbc:databend://databend:secret@0.0.0.0:8000/hello_databend";
        Properties properties=new Properties();
        properties.setProperty("user","test");
        properties.setProperty("password","secret");
        properties.setProperty("SSL","true");
        Connection connection=DriverManager.getConnection(url,properties);

// properties
        String url="jdbc:databend://databend:secret@0.0.0.0:8000/hello_databend?user=test&password=secret&SSL=true";
        Connection connection=DriverManager.getConnection(url);
```

### Parameter References

| Parameter              | Description                                                                                                               | Default       | example                                                                                                  |
|------------------------|---------------------------------------------------------------------------------------------------------------------------|---------------|----------------------------------------------------------------------------------------------------------|
| user                   | Databend user name                                                                                                        | none          | jdbc:databend://0.0.0.0:8000/hello_databend?user=test                                                    |
| password               | Databend user password                                                                                                    | none          | jdbc:databend://0.0.0.0:8000/hello_databend?password=secret                                              |
| SSL                    | Enable SSL                                                                                                                | false         | jdbc:databend://0.0.0.0:8000/hello_databend?SSL=true                                                     |
| sslmode                | SSL mode                                                                                                                  | disable       | jdbc:databend://0.0.0.0:8000/hello_databend?sslmode=enable                                               |
| copy_purge             | If True, the command will purge the files in the stage after they are loaded successfully into the table                  | false         | jdbc:databend://0.0.0.0:8000/hello_databend?copy_purge=true                                              |
| presigned_url_disabled | whether use presigned url to upload data, generally if you use local disk as your storage layer, it should be set as true | false         | jdbc:databend://0.0.0.0:8000/hello_databend?presigned_url_disabled=true                                  |
| wait_time_secs         | Restful query api blocking time, if the query is not finished, the api will block for wait_time_secs seconds              | 10            | jdbc:databend://0.0.0.0:8000/hello_databend?wait_time_secs=10                                            |
| max_rows_per_page      | the maximum rows per page in response data body                                                                           | 100000        | jdbc:databend://0.0.0.0:8000/default?max_rows_per_page=100000                                            |
| null_display           | null value display                                                                                                        | \N            | jdbc:databend://0.0.0.0:8000/hello_databend?null_display=null                                            |
| binary_format          | binary format, support hex and base64                                                                                     | hex           | jdbc:databend://0.0.0.0:8000/default?binary_format=hex                                                   |
| use_verify             | whether verify the server before establishing the connection                                                              | true          | jdbc:databend://0.0.0.0:8000/default?use_verify=true                                                     |
| debug                  | whether enable debug mode                                                                                                 | false         | jdbc:databend://0.0.0.0:8000/default?debug=true                                                          |
| load_balancing_policy | Specifies the load balancing policy for multi-host connections. Options are "disabled", "random", and "round_robin".      | disabled      | jdbc:databend://localhost:8000,localhost:8002,localhost:8003/default?load_balancing_policy=random        |
| max_failover_retry | Specifies the maximum number of retry attempts for failover connections.                                                  | 0             | jdbc:databend://localhost:7222,localhost:7223,localhost:7224,localhost:8000/default?max_failover_retry=4 |
| auto_discovery | Automatically discover possible cluster nodes in a databend query cluster                                                 | false         | jdbc:databend://0.0.0.0:8000/default?auto_discovery=true                                                 |
| node_discovery_interval | Minimum interval between two automatic node discovery actions in milliseconds                                             | 5 * 60 * 1000 | jdbc:databend://0.0.0.0:8000/default?node_discovery_interval=600000                                      |
| session_settings | set databend session settings                                                                                             | ""            | jdbc:databend://0.0.0.0:8000/default?session_settings="key1=value1,key2=value2"                          |
