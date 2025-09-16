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
    <version>0.4.0</version>
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
            try(ResultSet r = statement.getResultSet()){
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


# FileTransfer API

The `FileTransferAPI` interface provides a high-performance, Java-based mechanism for streaming data directly between your application and Databend's internal stage, eliminating the need for intermediate local files. It is designed for efficient bulk data operations.

## Key Features

* **Streaming Upload/Download:** Directly transfer data using `InputStream`, supporting large files without excessive memory consumption
* **Direct Table Loading:** Ingest data from streams or staged files directly into Databend tables using the `COPY INTO` command
* **Compression:** Supports on-the-fly compression and decompression during transfer to optimize network traffic
* **Flexible Data Ingestion:** Offers both stage-based and streaming-based methods for loading data into tables

## Core Methods

### `uploadStream`
Uploads a data stream as a single file to the specified internal stage.

**Parameters:**
- `stageName`: The stage which will receive the uploaded file
- `destPrefix`: The prefix of the file name in the stage
- `inputStream`: The input stream of the file data
- `destFileName`: The destination file name in the stage
- `fileSize`: The size of the file being uploaded
- `compressData`: Whether to compress the data during transfer

### `downloadStream`
Downloads a file from the internal stage and returns it as an `InputStream`.

**Parameters:**
- `stageName`: The stage which contains the file to download
- `sourceFileName`: The name of the file in the stage
- `decompress`: Whether to decompress the data during download

**Returns:** `InputStream` of the downloaded file content


### `loadStreamToTable` 
A versatile method to load data from a stream directly into a table, using either a staging or streaming approach.

Available with databend-jdbc >= 0.4 AND databend-query >= 1.2.791.

**Parameters:**
- `sql`: SQL statement with specific syntax for data loading
- `inputStream`: The input stream of the file data to load
- `fileSize`: The size of the file being loaded
- `loadMethod`: The loading method - "stage" or "streaming". `stage` method first upload file to a special path in user stage, while `steaming` method load data to while transforming data.

**Returns:** Number of rows successfully loaded

## Quick Start

The following example demonstrates how to upload data and load it into a table:

```java
// 1. Upload a file to the internal stage
Connection conn = DriverManager.getConnection("jdbc:databend://localhost:8000");
DatabendConnection api = conn.unwrap(DatabendConnection.class);

FileInputStream fileStream = new FileInputStream("data.csv");
api.uploadStream(
    "my_stage",
    "uploads/",
    fileStream,
    "data.csv",
    Files.size(Paths.get("data.csv")),
    true // Compress the data during upload
);
fileStream.close();

// 2. Load the staged file into a table
FileInputStream fileStream = new FileInputStream("data.csv");
String sql = "insert into my_table from @_databend_load file_format=(type=csv)"; // use special stage `_databend_load
api.loadStreamToTable(sql, file_stream, Files.size(Paths.get("data.csv")), "stage");
fileStream.close();
conn.close())


```

> **Important:** Callers are responsible for properly closing the provided `InputStream` objects after operations are complete.
