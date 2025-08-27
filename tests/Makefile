DATABEND_META_VERSION ?= nightly
DATABEND_QUERY_VERSION ?= nightly

default: run

run: test-core test-driver test-bendsql down

prepare:
	mkdir -p data/databend

up: prepare
	docker compose up --quiet-pull -d  --wait
	grep -q '127.0.0.1 minio' /etc/hosts || echo '127.0.0.1 minio' | sudo tee -a /etc/hosts > /dev/null
	curl  -u root: -XPOST "http://localhost:8000/v1/query" -H 'Content-Type: application/json' -d '{"sql": "select version()",  "pagination": { "wait_time_secs": 10}}'

down:
	docker compose down

