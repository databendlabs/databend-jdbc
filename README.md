# Databend JDBC
![Apache License 2.0](https://img.shields.io/badge/license-Apache%202.0-blue.svg)

## Prerequisites
The Databend JDBC driver requires Java 8 or later. 
If the minimum required version of Java is not installed on the client machines where the JDBC driver is installed, you must install either Oracle Java or OpenJDK.
## Installation
### Maven
Add following code block as a dependency
```xml
<dependency>
    <groupId>com.datafuse</groupId>
    <artifactId>databend-jdbc</artifactId>
    <version>1-SNAPSHOT</version>
</dependency>
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
        Statement statement = connection.createStatement();
        statement.execute("SELECT number from numbers(200000) order by number");
        ResultSet r = statement.getResultSet();
        r.next();
        for (int i = 1; i < 1000; i++) {
            r.next();
            System.out.println(r.getInt(1));
        }
        connection.close();
    }
}
```
