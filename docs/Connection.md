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

| Parameter              | Description                                                                                                               | Default | example                                                                 |
|------------------------|---------------------------------------------------------------------------------------------------------------------------|---------|-------------------------------------------------------------------------|
| user                   | Databend user name                                                                                                        | none    | jdbc:databend://0.0.0.0:8000/hello_databend?user=test                   |
| password               | Databend user password                                                                                                    | none    | jdbc:databend://0.0.0.0:8000/hello_databend?password=secret             |
| SSL                    | Enable SSL                                                                                                                | false   | jdbc:databend://0.0.0.0:8000/hello_databend?SSL=true                    |
| sslmode                | SSL mode                                                                                                                  | disable | jdbc:databend://0.0.0.0:8000/hello_databend?sslmode=enable              |
| copy_purge             | If True, the command will purge the files in the stage after they are loaded successfully into the table                  | false   | jdbc:databend://0.0.0.0:8000/hello_databend?copy_purge=true             |
| presigned_url_disabled | whether use presigned url to upload data, generally if you use local disk as your storage layer, it should be set as true | false   | jdbc:databend://0.0.0.0:8000/hello_databend?presigned_url_disabled=true |
| wait_time_secs         | Restful query api blocking time, if the query is not finished, the api will block for wait_time_secs seconds              | 10      | jdbc:databend://0.0.0.0:8000/hello_databend?wait_time_secs=10           |
| max_rows_in_buffer     | the maximum rows in server session buffer                                                                                 | 5000000 | jdbc:databend://0.0.0.0:8000/hello_databend?max_rows_in_buffer=5000000  |
| max_rows_per_page      | the maximum rows per page in response data body                                                                           | 100000  | jdbc:databend://0.0.0.0:8000/default?max_rows_per_page=100000           |
| connection_timeout     | okhttp connection_timeout param                                                                                           | 0       | jdbc:databend://0.0.0.0:8000/default?connection_timeout=100000          |
| query_timeout          | time that you wait a SQL execution                                                                                        | 90      | jdbc:databend://0.0.0.0:8000/default?query_timeout=120                  |
| null_display           | null value display                                                                                                        | \N      | jdbc:databend://0.0.0.0:8000/hello_databend?null_display=null           |
| binary_format          | binary format, support hex and base64                                                                                     | hex     | jdbc:databend://0.0.0.0:8000/default?binary_format=hex                  |
| use_verify             | whether verify the server before establishing the connection                                                              | true    | jdbc:databend://0.0.0.0:8000/default?use_verify=true                    |
| stage_name             | User specify stage name                                                                                                   | ~       | jdbc:databend://0.0.0.0:8000/default?stage_name=own_stage               |
