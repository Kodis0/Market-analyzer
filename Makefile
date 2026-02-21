.PHONY: run test lint format coverage check

run:
	python app.py -c config.yaml

test:
	python -m pytest tests/ -v

lint:
	ruff check .

format:
	ruff format .

coverage:
	python -m pytest --cov --cov-report=term-missing -q

check: lint test
