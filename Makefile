build:
	cargo build
	rm swellow/bin/swellow-linux-x86_64/swellow
	mv target/debug/swellow swellow/bin/swellow-linux-x86_64/swellow
	pip install .

test:
	cargo test
	pytest -vs