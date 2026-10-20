# Arrow value-decoding benchmark

An offline microbenchmark for `ArrowResultPage.getValue()`, using only the shaded
release jar and a JDK (17 recommended). It excludes HTTP, decompression, page
construction and server execution from the timed region. It is not an end-to-end
JMeter benchmark or a prediction of overall query latency.

Each scan reads 20,000 rows × 15 columns and consumes every returned value via a
checksum. Scenarios cover primitive/text columns and seven Decimal columns, each
with one or twenty record batches and periodic NULLs. Decimal still uses Arrow's
`DecimalVector.getObject()`; this PR does not optimize Decimal construction.

Build and run from the repository root:

```bash
mvn -pl databend-jdbc -DskipTests package
bash tests/benchmarks/arrow-values/run.sh "$PWD/databend-jdbc/target/databend-jdbc-0.4.10.jar"
```

For comparison, build the baseline commit's shaded jar in a separate worktree
and pass that jar to the same script. Use the same JDK and idle machine, alternate
baseline/candidate order, and compare multiple fresh JVMs (at least three).
Checksums must match between revisions for each scenario. Compare medians rather
than a single run. `WARMUPS` (default 10) and `ITERATIONS` (default 20) control the
number of full scans. This simple harness is not JMH; timing is diagnostic, not a
CI performance assertion.

## Local comparison

Baseline: `9dead58` (main including #429). macOS aarch64, Temurin 17.0.16,
512 MiB heap, default warmups/iterations, three fresh JVMs per revision with
alternating baseline/candidate order. Median results:

| Scenario | Batches | Baseline ns/value | Optimized ns/value | Time reduction |
| --- | ---: | ---: | ---: | ---: |
| Primitive/text | 1 | 32.99 | 20.67 | 37.3% |
| Primitive/text | 20 | 32.62 | 19.74 | 39.5% |
| Seven Decimal columns | 1 | 39.22 | 25.37 | 35.3% |
| Seven Decimal columns | 20 | 45.45 | 25.36 | 44.2% |

Checksums matched for every scenario across all six JVM runs. These measurements
include value hashing but exclude page creation, so they do not quantify the
per-page schema-classification cost or throughput for tiny results.
