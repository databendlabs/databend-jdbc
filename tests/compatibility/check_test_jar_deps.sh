#!/usr/bin/env bash
# Static gate: every class the -tests jar references must be resolvable from the
# shaded driver jar, the JDK, and the small set of third-party jars downstream
# compat suites put on the classpath.
#
# This is the cheap version of the release-jar run: it needs no Databend server,
# so it can run on every PR and catch test code that only compiles because
# Maven's test classpath happens to expose a shaded dependency.

set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/common.sh"

compat_build_jars
compat_download_libs

MAIN_JAR="$(compat_main_jar)"
TEST_JAR="$(compat_test_jar)"
MULTI_RELEASE="$(compat_multi_release)"

compat_log "driver jar: ${MAIN_JAR}"
compat_log "test jar:   ${TEST_JAR}"

JDEPS_OUTPUT="$(mktemp)"
JDEPS_STDERR="$(mktemp)"
trap 'rm -f "${JDEPS_OUTPUT}" "${JDEPS_STDERR}"' EXIT

set +e
jdeps \
    --multi-release "${MULTI_RELEASE:-base}" \
    -verbose:class \
    --class-path "${MAIN_JAR}:${COMPAT_LIB_DIR}/*" \
    "${TEST_JAR}" >"${JDEPS_OUTPUT}" 2>"${JDEPS_STDERR}"
JDEPS_EXIT=$?
set -e

# jdeps reports unresolved classes on stdout and still exits 0. A non-zero exit
# means jdeps itself could not run, which must not be mistaken for a clean check.
if [ "${JDEPS_EXIT}" -ne 0 ]; then
    echo "jdeps failed with exit ${JDEPS_EXIT}:" >&2
    cat "${JDEPS_STDERR}" >&2
    exit 1
fi

if ! grep -q "$(basename "${TEST_JAR}")" "${JDEPS_OUTPUT}"; then
    echo "jdeps produced no dependency records for ${TEST_JAR}" >&2
    exit 1
fi

# Class-level misses look like:
#   com.databend.jdbc.StatementUtilTest -> com.google.common.collect.ImmutableMap not found
# The archive-level "<jar> -> not found" summary line is dropped by $3 != "not".
MISSING="$(awk '$2 == "->" && $NF == "found" && $(NF-1) == "not" && $3 != "not" { print "  " $1 " -> " $3 }' "${JDEPS_OUTPUT}" | sort -u)"

RELOCATED=""
for prefix in ${COMPAT_RELOCATED_PREFIXES}; do
    hits="$(awk -v p="${prefix}." '$2 == "->" && index($3, p) == 1 { print "  " $1 " -> " $3 }' "${JDEPS_OUTPUT}" | sort -u)"
    if [ -n "${hits}" ]; then
        RELOCATED="${RELOCATED}${hits}
"
    fi
done

STATUS=0

if [ -n "${MISSING}" ]; then
    echo
    echo "Test jar references classes that are not on the release classpath:"
    echo "${MISSING}"
    echo
    echo "These tests pass under 'mvn test' but fail once the -tests jar runs against"
    echo "the shaded driver jar. Replace the dependency with JDK APIs or with something"
    echo "the driver jar exposes unrelocated."
    STATUS=1
fi

if [ -n "${RELOCATED}" ]; then
    echo
    echo "Test jar references shade-relocated packages:"
    printf '%s' "${RELOCATED}"
    echo
    echo "Relocated names are an implementation detail of the shaded jar and change"
    echo "between releases. Use the original package or a JDK API instead."
    STATUS=1
fi

if [ "${STATUS}" -eq 0 ]; then
    compat_log "test jar resolves against the release classpath"
fi

exit "${STATUS}"
