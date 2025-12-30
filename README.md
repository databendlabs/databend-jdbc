# Databend JDBC

![Apache License 2.0](https://img.shields.io/badge/license-Apache%202.0-blue.svg)
[![databend-jdbc](https://img.shields.io/maven-central/v/com.databend/databend-jdbc?style=flat-square)](https://central.sonatype.dev/artifact/com.databend/databend-jdbc/0.0.1)

## Highlights

- Databend-specific interfaces to stream files into tables or stages with `loadStreamToTable`, `uploadStream`, and `downloadStream`.
- Temporal APIs use session timezone to avoid depending on JVM default zone and support modern `java.time`.

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
    <version>0.4.2</version>
</dependency>
```

### Build from source

Note: build from source requires Java 11+, Maven 3.6.3+

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
        try ( Connection conn = DriverManager.getConnection("jdbc:databend://localhost:8000", "root", "");
              Statement statement = conn.createStatement()
            ) {
            statement.execute("SELECT number from numbers(200000) order by number");
            try(ResultSet rs = statement.getResultSet()){
                // ** We must call `rs.next()` otherwise the query may be canceled **
                while (rs.next()) {
                    System.out.println(r.getInt(1));
                }
            }
        }
    }
}
```

### Important Notes

1. Close Connection/Statement/ResultSet to release resources faster.
2. Because the `select`, `copy into`, `merge into` are query type SQL, they will return a `ResultSet` object, you must
   call `rs.next()` before accessing the data. Otherwise, the query may be canceled. If you do not want get the result,
   you can call `while(r.next(){})` to iterate over the result set.
3. For other SQL such as `create/drop table` non-query type SQL, you can call `statement.execute()` directly.


## Connection Parameters 

For detailed references, please take a look at the following Links:

1. [Connection Parameters](./docs/Connection.md) : detailed documentation about how to use connection parameters in a
   jdbc connection

## JDBC Java type mapping
The Databend type is mapped to Java type as follows:

| Databend Type | Java Type      |
|---------------|----------------|
| TINYINT       | Byte           |
| SMALLINT      | Short          |
| INT           | Integer        |
| BIGINT        | Long           |
| UInt8         | Short          |
| UInt16        | Integer        |
| UInt32        | Long           |
| UInt64        | BigInteger     |
| Float32       | Float          |
| Float64       | Double         |
| Decimal       | BigDecimal     |
| String        | String         |
| Date          | LocalDate      |
| Timestamp     | ZonedDateTime  |
| Timestamp_TZ  | OffsetDateTime |
| Interval      | String         |
| Geometry      | byte[]         |
| Bitmap        | byte[]         |
| Array         | String         |
| Tuple         | String         |
| Map           | String         |
| VARIANT       | String         |

### Temporal types

we recommend using `java.time` to avoid ambiguity and set/get values via these APIs:

```
void setObject(int parameterIndex, Object x)
<T> T getObject(int columnIndex, Class<T> type)
```

- TIMESTAMP_TZ and TIMESTAMP map to `OffsetDateTime`, `ZonedDateTime`, `Instant` and `LocalDateTime` (TIMESTAMP_TZ can return `OffsetDateTime` but not `ZonedDateTime`).
- Date maps to `LocalDate`
- When parameters do not contain a timezone, Databend uses the session timezone (not the JVM zone) when storing/returning dates on databend-jdbc ≥ 0.4.3 AND databend-query ≥1.2.844.

old Timestamp/Date are also supported, note that:

- `getTimestamp(int, Calendar cal)` is equivalent to `getTimestamp(int)` (the cal is omitted) and
`getObject(int, Instant.classes).toTimestamp()`
- `setTimestamp(int, Calendar cal)` is diff with `setTimestamp(int)`, the epoch is adjusted according to timezone in cal
- `setDate`/`getDate` still use the JVM timezone, `getDate(1)` is equivalent to `Date.valueOf(getObject(1, LocalDate.class))`, `setDate(1, date)` is equivalent to `setObject(1, date.toLocalDate())`.


# Unwrapping to Databend-specific interfaces 

## interface DatabendConnection

The following code shows how to unwrap a JDBC Connection object to expose the methods of the DatabendConnection interface.

```java
import com.databend.jdbc.DatabendConnection;
Connection conn = DriverManager.getConnection("jdbc:databend://localhost:8000");
DatabendConnection databendConnection = conn.unwrap(DatabendConnection.class);
```

### method `loadStreamToTable` 

```java
int loadStreamToTable(String sql, InputStream inputStream, long fileSize, LoadMethod loadMethod) throws SQLException;
```

Load data from a stream directly into a table, using either a staging or streaming approach.

Available with databend-jdbc >= 0.4.1 AND databend-query >= 1.2.791.

**Parameters:**
- `sql`: SQL statement with specific syntax for data loading, use special stage `_databend_load`
- `inputStream`: The input stream of the file data to load
- `fileSize`: The size of the file being loaded
- `loadMethod`: LoadMethod.STREAMING or LoadMethod.STAGE
  - `STAGE`: first upload file to a special path in user stage, then load the file in stage in to table, Limited by the max object size of storage of the stage.
    - the upload method is determined by connection parameter `presigned_url_disabled`.
  - `STREAMING` load data to while transforming data in one http request. Limited by server memory when load large Parquet/Orc file, whose meta is at the file end.

**Returns:** Number of rows successfully loaded

example:


```java
import com.databend.jdbc.DatabendConnection;
try(Connection conn = DriverManager.getConnection("jdbc:databend://localhost:8000")) {
    try(FileInputStream fileStream = new FileInputStream("data.csv")) {
        // unwrap 
        DatabendConnection databendConnection = conn.unwrap(DatabendConnection.class);
        
        // use special stage `_databend_load`
        String sql = "insert into my_table from @_databend_load file_format=(type=csv)";
        
        databendConnection.loadStreamToTable(sql, fileStream, Files.size(Paths.get("data.csv")), DatabendConnection.LoadMethod.STAGE);
    }
}

```

### method `uploadStream` and `downloadStream`

Upload a `InputStream` as a single file in the stage.

the upload method is determined by connection parameter `presigned_url_disabled`.

```java
void uploadStream(InputStream inputStream, String stageName, String destPrefix, String destFileName, long fileSize, boolean compressData) throws SQLException;
```

Download a single file in the stage as `InputStream`

```
InputStream downloadStream(String stageName, String filePathInStage) throws SQLException;
```
