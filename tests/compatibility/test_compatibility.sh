#!/bin/bash

set -exo pipefail

#curl -sSLfo ./testng.jar https://repo.maven.apache.org/maven2/org/testng/testng/7.11.0/testng-7.11.0.jar
#curl -sSLfo ./semver4j.jar https://repo1.maven.org/maven2/com/vdurmont/semver4j/3.1.0/semver4j-3.1.0.jar
#curl -sSLfo ./jcommander.jar https://repo1.maven.org/maven2/org/jcommander/jcommander/1.83/jcommander-1.83.jar
#curl -sSLfo ./jts-core.jar https://repo1.maven.org/maven2/org/locationtech/jts/jts-core/1.19.0/jts-core-1.19.0.jar
#curl -sSLfo ./slf4j-api.jar https://repo1.maven.org/maven2/org/slf4j/slf4j-api/2.0.16/slf4j-api-2.0.16.jar

TEST_VER=0.4.0
JDBC_VER=${DATABEND_TEST_DRIVER_VERSION:-0.4.0}

# for dev
# 1. run `mvn clean package -DskipTests`
# 2. set TEST_DEV=1
# 3. unset DATABEND_TEST_DRIVER_VERSION

TEST_DEV=${TEST_DEV:-0}

if [ "$TEST_DEV" = "1" ]; then
    curl -sSLfo ./databend-jdbc-tests.jar "https://github.com/databendlabs/databend-jdbc/releases/download/v${TEST_VER}/databend-jdbc-${TEST_VER}-tests.jar"
else
    cp ../../databend-jdbc/target/databend-jdbc-${TEST_VER}-tests.jar databend-jdbc-tests.jar
fi

if [ -z "$DATABEND_TEST_DRIVER_VERSION" ]; then
    cp ../../databend-jdbc/target/databend-jdbc-${JDBC_VER}.jar databend-jdbc.jar
else
    curl -sSLfo "./databend-jdbc-${JDBC_VER}.jar" "https://github.com/databendlabs/databend-jdbc/releases/download/v${JDBC_VER}/databend-jdbc-${JDBC_VER}.jar"
fi

export DATABEND_TEST_DRIVER_VERSION=$JDBC_VER
java -Dlogback.logger.root=INFO -cp "testng.jar:slf4j-api.jar:databend-jdbc-${JDBC_VER}.jar:databend-jdbc-tests.jar:jcommander.jar:semver4j.jar" org.testng.TestNG testng.xml
