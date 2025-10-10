test:
	chmod +x ./venv/bin/activate
	./venv/bin/activate
	cargo build
	rm swellow/bin/swellow-linux-x86_64/swellow
	mv target/debug/swellow swellow/bin/swellow-linux-x86_64/swellow
	pip install .
	pytest -vs