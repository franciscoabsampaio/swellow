PG_VERSION := $(shell cat PG_VERSION)

init:
	git config core.hooksPath .githooks
	chmod +x .githooks/pre-commit

build:
	cargo build
	rm -f swellow/bin/swellow-linux-x86_64/swellow
	sudo cp target/debug/swellow /usr/local/bin/
	cp target/debug/swellow swellow/bin/swellow-linux-x86_64/swellow
	. venv/bin/activate && pip install .

test:
	cargo test
	. venv/bin/activate && pytest -vs

pg:
	docker run --name pg -e POSTGRES_USER=pguser -e POSTGRES_PASSWORD=pgpass -e POSTGRES_DB=mydb -p 5432:5432 -d postgres:$(PG_VERSION)

spark-delta:
	docker run --name spark-delta -p 15002:15002 -d franciscoabsampaio/spark-connect-server:delta

spark-iceberg:
	docker run --name spark-iceberg -p 15003:15002 -d franciscoabsampaio/spark-connect-server:iceberg