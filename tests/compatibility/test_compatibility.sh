#!/bin/bash

set -ex

curl -sSLfo ./testng.jar https://repo.maven.apache.org/maven2/org/testng/testng/7.11.0/testng-7.11.0.jar
curl -sSLfo ./semver4j.jar https://repo1.maven.org/maven2/com/vdurmont/semver4j/3.1.0/semver4j-3.1.0.jar
curl -sSLfo ./jcommander.jar https://repo1.maven.org/maven2/org/jcommander/jcommander/1.83/jcommander-1.83.jar
curl -sSLfo ./jts-core.jar https://repo1.maven.org/maven2/org/locationtech/jts/jts-core/1.19.0/jts-core-1.19.0.jar
curl -sSLfo ./slf4j-api.jar https://repo1.maven.org/maven2/org/slf4j/slf4j-api/2.0.16/slf4j-api-2.0.16.jar

original_dir=$(pwd)
cd ../..
# got 1 if not in java project
CURRENT_VERSION=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)
cd "$original_dir"

TEST_SIDE=${TEST_SIDE:-server}
TEST_VER=${DATABEND_JDB_TEST_VERSION:-$CURRENT_VERSION}
JDBC_VER=${DATABEND_JDBC_VERSION:-$CURRENT_VERSION}

JDBC_JAR="databend-jdbc-${JDBC_VER}.jar"
JDBC_TEST_JAR="databend-jdbc-${TEST_VER}-tests.jar"

if [ "$TEST_SIDE" = "server" ]; then
    curl -sSLfO "https://github.com/databendlabs/databend-jdbc/releases/download/v${TEST_VER}/${JDBC_TEST_JAR}"
else
    cp "../../databend-jdbc/target/${JDBC_TEST_JAR}" .
fi

if [ -z "DATABEND_JDBC_VERSION" ]; then
    # test main branch
    cp "../../databend-jdbc/target/${JDBC_JAR}" .
else
    curl -sSLfO "https://github.com/databendlabs/databend-jdbc/releases/download/v${JDBC_VER}/${JDBC_JAR}"
fi

export DATABEND_JDBC_VERSION=$JDBC_VER
java -Dlogback.logger.root=INFO -cp "testng.jar:slf4j-api.jar:${JDBC_JAR}:${JDBC_TEST_JAR}:jcommander.jar:semver4j.jar" org.testng.TestNG testng.xml
