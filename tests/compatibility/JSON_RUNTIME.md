# JSON runtime compatibility smoke test

The driver advertises Java 8 support, but building with `maven.compiler.release=8`
only constrains the project's own classes. Shaded dependencies may still require
a newer JVM. The existing TestNG release-jar harness requires Java 11+ itself,
so it cannot check Java 8 consumers.

`Java8JsonSmoke.java` uses only JDK APIs at compile time. At runtime its classpath
contains only the compiled smoke test and the shaded driver jar. A loopback HTTP
server supplies deterministic login and paginated JSON responses; no Databend
installation or additional dependency jars are needed.

It exercises class loading/reflection, JDBC service-provider discovery, connection/login, default JSON
selection against an Arrow-capable server, statement execution, metadata, two-page
result iteration, explicit JSON with a prepared statement, and resource closing.
On Java 8 it also checks that requesting Arrow is rejected when the connection is created,
before any query is sent, and that an unsolicited Arrow response produces an actionable
SQLException rather than a linkage error. It is not an Arrow decoding test or a replacement for live-server
integration tests.

## Run locally

Build with JDK 17 (as the release workflow does):

```sh
mvn -B -ntp clean package -DskipTests -Dmaven.javadoc.skip=true -Dgpg.skip=true
```

Then select the consumer JDK by setting `JAVA_HOME` and updating `PATH`, and run:

```sh
bash tests/compatibility/run_json_smoke.sh
```

The script does not rebuild the jar. Both `java` and `javac` must come from the
selected JDK. CI runs the same check on JDK 8, JDK 11 and JDK 17, with failures propagated
normally (no `continue-on-error`).
