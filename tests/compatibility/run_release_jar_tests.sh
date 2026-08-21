#!/usr/bin/env bash
# Runtime gate: run the IT suite the way a release consumer does.
#
# Instead of 'mvn test', the locally built shaded driver jar and -tests jar are
# put on a bare classpath together with the third-party jars downstream compat
# suites provide, and TestNG is invoked directly. A Databend server must be
# reachable (tests/Makefile up, or DATABEND_TEST_CONN_PORT pointing elsewhere).

set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

COMPAT_GROUPS="${COMPAT_GROUPS:-IT}"
COMPAT_EXCLUDED_GROUPS="${COMPAT_EXCLUDED_GROUPS:-FLAKY,cluster,MULTI_HOST}"
COMPAT_USER_TIMEZONE="${COMPAT_USER_TIMEZONE:-Asia/Shanghai}"

compat_require_java
compat_build_jars
compat_download_libs

MAIN_JAR="$(compat_main_jar)"
TEST_JAR="$(compat_test_jar)"

# Keep runs of different group selections in separate directories so a second
# pass (for example UNIT after IT) does not clobber the first one's report.
COMPAT_RUN_TAG="$(printf '%s' "${COMPAT_GROUPS}" \
    | tr ',[:upper:]' '-[:lower:]' \
    | tr -c 'a-z0-9_-' '-')"
if [ -z "${COMPAT_RUN_TAG}" ]; then
    COMPAT_RUN_TAG="all"
fi
SUITE_FILE="${COMPAT_GENERATED_DIR}/testng-${COMPAT_RUN_TAG}.xml"
COMPAT_OUTPUT_DIR="${COMPAT_OUTPUT_ROOT}/${COMPAT_RUN_TAG}"

TEST_CLASSES="$(compat_test_classes "${TEST_JAR}")"
if [ -z "${TEST_CLASSES}" ]; then
    echo "no test classes discovered in ${TEST_JAR}" >&2
    exit 1
fi
CLASS_COUNT="$(printf '%s\n' "${TEST_CLASSES}" | wc -l | tr -d '[:space:]')"

mkdir -p "${COMPAT_GENERATED_DIR}"
case "${COMPAT_OUTPUT_DIR}" in
    "${COMPAT_OUTPUT_ROOT}/"*) ;;
    *)
        echo "refusing to clean output directory outside ${COMPAT_OUTPUT_ROOT}: ${COMPAT_OUTPUT_DIR}" >&2
        exit 1
        ;;
esac
rm -rf "${COMPAT_OUTPUT_DIR}"
mkdir -p "${COMPAT_OUTPUT_DIR}"

{
    echo '<!DOCTYPE suite SYSTEM "https://testng.org/testng-1.0.dtd" >'
    echo '<suite name="DatabendJdbcCompatTests" verbose="1" parallel="none">'
    echo '  <test name="AllTests">'
    echo '    <groups>'
    echo '      <run>'
    for group in $(echo "${COMPAT_GROUPS}" | tr ',' ' '); do
        echo "        <include name=\"${group}\"/>"
    done
    for group in $(echo "${COMPAT_EXCLUDED_GROUPS}" | tr ',' ' '); do
        echo "        <exclude name=\"${group}\"/>"
    done
    echo '      </run>'
    echo '    </groups>'
    echo '    <classes>'
    printf '%s\n' "${TEST_CLASSES}" | while read -r class_name; do
        echo "      <class name=\"${class_name}\"/>"
    done
    echo '    </classes>'
    echo '  </test>'
    echo '</suite>'
} >"${SUITE_FILE}"

compat_log "driver jar: ${MAIN_JAR}"
compat_log "test jar:   ${TEST_JAR}"
compat_log "suite:      ${SUITE_FILE} (${CLASS_COUNT} classes, groups=${COMPAT_GROUPS})"

JAVA_ARGS=("-Duser.timezone=${COMPAT_USER_TIMEZONE}")
if [ "${DATABEND_JDBC_TEST_QUERY_RESULT_FORMAT:-}" = "arrow" ]; then
    # Arrow needs the same JVM openings the arrow-tests Maven profile sets.
    JAVA_ARGS+=(--add-opens=java.base/java.nio=ALL-UNNAMED -Dio.netty.tryReflectionSetAccessible=true)
fi

set +e
java "${JAVA_ARGS[@]}" \
    -cp "${COMPAT_LIB_DIR}/*:${MAIN_JAR}:${TEST_JAR}" \
    org.testng.TestNG -d "${COMPAT_OUTPUT_DIR}" "${SUITE_FILE}"
EXIT_CODE=$?
set -e

# TestNG's exit code is a bitmask (org.testng.internal.ExitCode):
#   1 FAILED, 2 SKIPPED, 4 FAILED_WITHIN_SUCCESS, 8 HAS_NO_TEST.
# Skips are expected (version-gated and Arrow-gated tests call SkipException), so
# a skip-only run passes. Everything else must stay non-zero. HAS_NO_TEST matters
# most here: when a test class cannot be instantiated off the release classpath,
# TestNG aborts the suite, runs nothing, and reports 8 -- exactly the #414 failure.
if [ "${EXIT_CODE}" -ne 0 ]; then
    if [ $((EXIT_CODE & 8)) -ne 0 ]; then
        compat_log "TestNG ran no tests (exit ${EXIT_CODE}); a test class likely failed to load from the release jars"
    elif [ $((EXIT_CODE & 1)) -ne 0 ] || [ $((EXIT_CODE & 4)) -ne 0 ]; then
        compat_log "TestNG reported failures (exit ${EXIT_CODE})"
    elif [ "${EXIT_CODE}" -eq 2 ]; then
        compat_log "TestNG reported skipped tests and no failures, treating as success"
        EXIT_CODE=0
    else
        compat_log "TestNG exited ${EXIT_CODE}"
    fi
fi

exit "${EXIT_CODE}"
