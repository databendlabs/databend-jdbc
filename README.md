# Databend JDBC

![Apache License 2.0](https://img.shields.io/badge/license-Apache%202.0-blue.svg)
[![databend-jdbc](https://img.shields.io/maven-central/v/com.databend/databend-jdbc?style=flat-square)](https://central.sonatype.dev/artifact/com.databend/databend-jdbc/0.0.1)

## Prerequisites

The Databend JDBC driver requires Java 8 or later.
If the minimum required version of Java is not installed on the client machines where the JDBC driver is installed, you
must install either Oracle Java or OpenJDK.

## Installation

### Maven

Add following code block as a dependency

```xml

<dependency>
    <groupId>com.databend</groupId>
    <artifactId>databend-jdbc</artifactId>
    <version>0.2.6</version>
</dependency>
```

### Build from source

```shell
cd databend-jdbc
mvn clean install -DskipTests
```

### Download jar from maven central

```shell
You can download the latest version of the databend-jdbc driver [here](https://repo1.maven.org/maven2/com/databend/databend-jdbc/).
```

## How to use

```java
import java.sql.SQLException;
import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;

public class Main {
    public static void main(String[] args) throws SQLException {
        Connection conn = DriverManager.getConnection("jdbc:databend://localhost:8000", "root", "");
        Statement statement = conn.createStatement();
        statement.execute("SELECT number from numbers(200000) order by number");
        ResultSet r = statement.getResultSet();
        // ** We must call `rs.next()` otherwise the query may be canceled **
        while (rs.next()) {
            System.out.println(r.getInt(1));
        }
        conn.close();
    }
}
```

### Important Notes

1. Because the `select`, `copy into`, `merge into` are query type SQL, they will return a `ResultSet` object, you must
   call `rs.next()` before accessing the data. Otherwise, the query may be canceled. If you do not want get the result,
   you can call `while(r.next(){})` to iterate over the result set.
2. For other SQL such as `create/drop table` non-query type SQL, you can call `statement.execute()` directly.

## JDBC Java type mapping
The Databend type is mapped to Java type as follows:

| Databend Type | Java Type  |
|---------------|------------|
| TINYINT       | Byte       |
| SMALLINT      | Short      |
| INT           | Integer    |
| BIGINT        | Long       |
| UInt8         | Short      |
| UInt16        | Integer    |
| UInt32        | Long       |
| UInt64        | BigInteger |
| Float32       | Float      |
| Float64       | Double     |
| String        | String     |
| Date          | String     |
| TIMESTAMP     | String     |
| Bitmap        | byte[]     |
| Array         | String     |
| Decimal       | BigDecimal |
| Tuple         | String     |
| Map           | String     |
| VARIANT       | String     |

For detailed references, please take a look at the following Links:

1. [Connection Parameters](./docs/Connection.md) : detailed documentation about how to use connection parameters in a
   jdbc connection
