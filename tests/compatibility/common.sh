#!/usr/bin/env bash
# Shared helpers for the release-jar compatibility harness.
#
# The harness reproduces how downstream projects (notably databend's compat CI)
# consume our artifacts: the shaded driver jar plus the -tests jar are put on a
# bare classpath and TestNG is invoked directly. Maven's test classpath is not
# involved, so anything the test code pulls from a provided/relocated dependency
# blows up here the same way it blows up after a release.

set -euo pipefail

COMPAT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${COMPAT_DIR}/../.." && pwd)"
JDBC_TARGET_DIR="${REPO_ROOT}/databend-jdbc/target"
COMPAT_LIB_DIR="${COMPAT_DIR}/lib"
COMPAT_GENERATED_DIR="${COMPAT_DIR}/generated"
COMPAT_OUTPUT_ROOT="${COMPAT_DIR}/test-output"

# Mirrors JDBC_TEST_LIBS in databend/tests/nox/noxfile.py, minus the JUnit console
# launcher (TestNG suites do not need it). Keeping this list narrow keeps the check
# strict: anything not here and not in the driver jar is a release-time failure.
COMPAT_LIB_URLS="
https://repo.maven.apache.org/maven2/org/testng/testng/7.11.0/testng-7.11.0.jar
https://repo1.maven.org/maven2/com/vdurmont/semver4j/3.1.0/semver4j-3.1.0.jar
https://repo1.maven.org/maven2/org/jcommander/jcommander/1.83/jcommander-1.83.jar
https://repo1.maven.org/maven2/org/locationtech/jts/jts-core/1.19.0/jts-core-1.19.0.jar
https://repo1.maven.org/maven2/org/slf4j/slf4j-api/2.0.16/slf4j-api-2.0.16.jar
https://repo1.maven.org/maven2/org/slf4j/slf4j-simple/2.0.13/slf4j-simple-2.0.13.jar
"

# Packages the shade plugin relocates. Test code must never reference them.
COMPAT_RELOCATED_PREFIXES="
com.databend.jdbc.com.fasterxml.jackson
com.databend.jdbc.org.slf4j
com.databend.jdbc.org.apache.commons.lang3
com.google.shaded.common
"

compat_log() {
    echo "[compat] $*"
}

compat_build_jars() {
    if [ "${COMPAT_SKIP_BUILD:-}" = "1" ]; then
        compat_log "COMPAT_SKIP_BUILD=1, reusing jars in ${JDBC_TARGET_DIR}"
        return
    fi

    compat_log "packaging shaded driver jar and test jar"
    (cd "${REPO_ROOT}" && mvn -B -ntp clean package -DskipTests -Dmaven.javadoc.skip=true -Dgpg.skip=true)
}

# Echoes the shaded driver jar, skipping the sources/tests/javadoc/pre-shade ones.
compat_main_jar() {
    local jar
    for jar in "${JDBC_TARGET_DIR}"/databend-jdbc-*.jar; do
        [ -e "${jar}" ] || continue
        case "$(basename "${jar}")" in
            original-*|*-sources.jar|*-tests.jar|*-javadoc.jar) continue ;;
        esac
        echo "${jar}"
        return
    done
    echo "no driver jar found in ${JDBC_TARGET_DIR}, run mvn package first" >&2
    return 1
}

compat_test_jar() {
    local jar
    for jar in "${JDBC_TARGET_DIR}"/databend-jdbc-*-tests.jar; do
        [ -e "${jar}" ] || continue
        echo "${jar}"
        return
    done
    echo "no test jar found in ${JDBC_TARGET_DIR}, run mvn package first" >&2
    return 1
}

compat_download_libs() {
    mkdir -p "${COMPAT_LIB_DIR}"
    local url filename
    for url in ${COMPAT_LIB_URLS}; do
        filename="$(basename "${url}")"
        if [ -f "${COMPAT_LIB_DIR}/${filename}" ]; then
            continue
        fi
        compat_log "downloading ${filename}"
        curl -sSLfo "${COMPAT_LIB_DIR}/${filename}" "${url}"
    done
}

# jdeps needs an explicit release for multi-release jars such as slf4j-api.
compat_multi_release() {
    java -XshowSettings:properties -version 2>&1 \
        | awk -F'= *' '/java.specification.version/ { gsub(/[^0-9]/, "", $2); print $2; exit }'
}

# TestNG class names contained in the test jar, mirroring databend's discovery
# rules so the generated suite matches what the compat CI runs.
compat_test_classes() {
    local test_jar="$1"
    jar tf "${test_jar}" \
        | grep '\.class$' \
        | grep -v '\$' \
        | sed -e 's/\.class$//' -e 's#/#.#g' \
        | awk -F. '{
              name = $NF
              if (name == "Compatibility" || name == "Utils") next
              if (name ~ /^Test/ || name ~ /Test$/) print
          }' \
        | sort -u
}
