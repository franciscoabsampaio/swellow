init:
	git config core.hooksPath .githooks
	chmod +x .githooks/pre-commit

build:
	cargo build
	rm swellow/bin/swellow-linux-x86_64/swellow
	mv target/debug/swellow swellow/bin/swellow-linux-x86_64/swellow
	. venv/bin/activate && pip install .

test:
	cargo test
	. venv/bin/activate && pytest -vs

pg:
	docker run --name pg -e POSTGRES_PASSWORD=postgres -p 5432:5432 -d postgres

spark-delta:
	docker run --name spark-delta -p 15002:15002 -d franciscoabsampaio/spark-connect-server:delta

spark-iceberg:
	docker run --name spark-iceberg -p 15003:15002 -d franciscoabsampaio/spark-connect-server:iceberg