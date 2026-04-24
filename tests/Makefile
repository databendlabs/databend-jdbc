DATABEND_META_VERSION ?= nightly
DATABEND_QUERY_VERSION ?= nightly
# Set DATABEND_JDBC_TEST_QUERY_RESULT_FORMAT=arrow to force Arrow result pages in tests.
# When Arrow mode is enabled, make test appends the required JVM options automatically.
# If you run Maven directly, set JAVA_TOOL_OPTIONS yourself as well.
DATABEND_JDBC_TEST_QUERY_RESULT_FORMAT ?=
DATABEND_JDBC_ARROW_JAVA_TOOL_OPTIONS ?= --add-opens=java.base/java.nio=ALL-UNNAMED -Dio.netty.tryReflectionSetAccessible=true
TEST_MVN_ARGS ?= -DexcludedGroups=FLAKY

TEST_JAVA_TOOL_OPTIONS := $(strip $(JAVA_TOOL_OPTIONS))
ifneq ($(filter arrow,$(DATABEND_JDBC_TEST_QUERY_RESULT_FORMAT)),)
TEST_JAVA_TOOL_OPTIONS := $(strip $(TEST_JAVA_TOOL_OPTIONS) $(DATABEND_JDBC_ARROW_JAVA_TOOL_OPTIONS))
endif

prepare:
	mkdir -p data/databend

up: prepare
	docker compose up --quiet-pull -d  --wait
	grep -q '127.0.0.1 minio' /etc/hosts || echo '127.0.0.1 minio' | sudo tee -a /etc/hosts > /dev/null
	curl  -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d '{"sql": "select version()",  "pagination": { "wait_time_secs": 10}}'

down:
	docker compose down

test:
	cd .. && \
	JAVA_TOOL_OPTIONS='$(TEST_JAVA_TOOL_OPTIONS)' \
	DATABEND_JDBC_TEST_QUERY_RESULT_FORMAT='$(DATABEND_JDBC_TEST_QUERY_RESULT_FORMAT)' \
	mvn -pl databend-jdbc test $(TEST_MVN_ARGS)
