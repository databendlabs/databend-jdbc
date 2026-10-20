#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
JAR="${1:?Usage: run.sh /path/to/shaded-driver.jar}"
CLASSES="$(mktemp -d)"
trap 'rm -rf "${CLASSES}"' EXIT

javac -cp "${JAR}" -d "${CLASSES}" \
    "${ROOT}/tests/benchmarks/arrow-values/ArrowValuesBenchmark.java"
java --add-opens=java.base/java.nio=ALL-UNNAMED \
    -Xms512m -Xmx512m -Dwarmups="${WARMUPS:-10}" -Diterations="${ITERATIONS:-20}" \
    -cp "${CLASSES}:${JAR}" com.databend.jdbc.internal.query.ArrowValuesBenchmark
