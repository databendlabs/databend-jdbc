#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/../../.." && pwd)
WORK=$(mktemp -d "${TMPDIR:-/tmp}/jdbc-lz4-benchmark.XXXXXX")
trap 'rm -rf "$WORK"' EXIT
cd "$ROOT"

# Benchmark the same relocated, Java-only artifact that users receive.
if ! mvn -q -pl databend-jdbc -DskipTests package >"$WORK/build.log" 2>&1; then
    cat "$WORK/build.log" >&2
    exit 1
fi
JAR=$(find "$ROOT/databend-jdbc/target" -maxdepth 1 -name 'databend-jdbc-*.jar' \
    ! -name '*-sources.jar' ! -name '*-javadoc.jar' ! -name '*-tests.jar' | head -1)
javac -cp "$JAR" -d "$WORK" tests/benchmarks/arrow-lz4/ArrowLz4Benchmark.java

run() {
    java "-Xmx${HEAP:-1g}" --add-opens=java.base/java.nio=ALL-UNNAMED \
        -cp "$JAR:$WORK" com.databend.jdbc.internal.query.ArrowLz4Benchmark "$@"
}

if [[ "${1:-verify}" == verify ]]; then
    run verify
elif [[ "$1" == matrix ]]; then
    echo 'variant,pattern,output_bytes,frame_bytes,buffers_per_batch,MiB_per_s,heap_bytes_per_buffer,gc_count,gc_ms'
    for pattern in repeated mixed random; do
        for batch in 1 16; do
            for variant in arrow commons-stream direct; do
                run "$variant" "${SIZE:-1048576}" "$pattern" "$batch" \
                    "${MEASURE_SECONDS:-2}"
            done
        done
    done
else
    run "$@"
fi
