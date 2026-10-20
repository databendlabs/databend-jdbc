# Arrow LZ4 release-jar smoke test

`ArrowLz4JarSmoke.java` is compiled and run against only the packaged shaded driver
jar and the JDK, never Maven's test classpath or `target/classes`.

It checks that relocation left no `net/jpountz/` entries, native libraries are
absent, Java LZ4/xxhash implementations load after jar minimization, and JNI isn't
loaded. Fifteen size/compressibility fixtures exercise correct off-heap decoding,
block/content checksums and multi-block frames. No Databend service is required.

Run with JDK 11+ after packaging with JDK 17:

```sh
mvn -B -ntp clean package -DskipTests -Dmaven.javadoc.skip=true -Dgpg.skip=true
bash tests/compatibility/run_arrow_lz4_smoke.sh
```

The Compatibility Test workflow runs the same script after the release test-jar
dependency check, reusing the packaged jar. Failures fail the CI job normally.

This is an artifact correctness test, not a timing benchmark or a substitute for
the codec's malformed-input and linked-block unit tests. Its fixtures use Commons'
frame writer and do not claim to represent real Databend pages or lz4_flex output.
