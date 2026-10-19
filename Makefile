.PHONY: install test run lint clean

install:
	python3 -m pip install -r requirements.txt

test:
	pytest -v tests/

run:
	python3 extract_hdb_resale.py --month 2024-01

run-historical:
	python3 extract_hdb_resale.py --month 2013-05

clean:
	rm -rf __pycache__ src/__pycache__ tests/__pycache__ .pytest_cache data/output/*.parquet
