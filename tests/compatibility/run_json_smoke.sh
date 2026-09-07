#!/usr/bin/env bash
# Package on a modern JDK first; run this script with the runtime JDK under test.
# No TestNG: the existing release-jar harness itself requires Java 11+.
set -euo pipefail
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

MAIN_JAR="$(compat_main_jar)"
CLASSES="$(mktemp -d)"
trap 'rm -rf "${CLASSES}"' EXIT

java -version
compat_log "JSON smoke test using only ${MAIN_JAR} and the JDK"
javac -d "${CLASSES}" "${COMPAT_DIR}/Java8JsonSmoke.java"
java -cp "${CLASSES}:${MAIN_JAR}" Java8JsonSmoke
