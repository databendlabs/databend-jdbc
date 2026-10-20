#!/usr/bin/env bash
# Build/package first; compile and run against only the shipped jar, not Maven's test classpath.
set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

compat_require_java
MAIN_JAR="$(compat_main_jar)"
CLASSES="$(mktemp -d)"
trap 'rm -rf "${CLASSES}"' EXIT

java -version
compat_log "Arrow LZ4 smoke test using only ${MAIN_JAR} and the JDK"
javac -cp "${MAIN_JAR}" -d "${CLASSES}" "${COMPAT_DIR}/ArrowLz4JarSmoke.java"
java --add-opens=java.base/java.nio=ALL-UNNAMED \
    -Dio.netty.tryReflectionSetAccessible=true \
    -cp "${MAIN_JAR}:${CLASSES}" \
    com.databend.jdbc.internal.query.ArrowLz4JarSmoke "${MAIN_JAR}"
