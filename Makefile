# Read PG_VERSION from the file
PG_VERSION := $(shell cat PG_VERSION)
# Use empty string if TEST is not set
TEST ?=

init:
	git config core.hooksPath .githooks
	chmod +x .githooks/*
	chmod +x scripts/*.sh

build:
	cargo build
	rm -f swellow/bin/swellow-linux-x86_64/swellow
	sudo cp target/debug/swellow /usr/local/bin/
	cp target/debug/swellow swellow/bin/swellow-linux-x86_64/swellow
	. venv/bin/activate && pip install .

# Run a specific test by setting TEST=<test>
# Example: make test TEST=test_snapshot
test:
	cargo test
	. venv/bin/activate && pytest -vs tests/test_module.py$$( [ -n "$(TEST)" ] && echo "::$(TEST)" )

pg:
	docker run --name pg -e POSTGRES_USER=pguser -e POSTGRES_PASSWORD=pgpass -e POSTGRES_DB=mydb -p 5432:5432 -d postgres:$(PG_VERSION)

spark-delta:
	docker run --name spark-delta -p 15002:15002 -d franciscoabsampaio/spark-connect-server:delta

spark-iceberg:
	docker run --name spark-iceberg -p 15003:15002 -d franciscoabsampaio/spark-connect-server:iceberg