.PHONY: install lint format typecheck test demo docker check

install:  ## create a dev environment
	pip install -e ".[dev]"
	pre-commit install

lint:
	ruff check .
	ruff format --check .

format:
	ruff check --fix .
	ruff format .

typecheck:
	mypy labs

test:
	pytest

demo:  ## run every lab end to end
	spark-labs

docker:
	docker build -t spark-pipeline-labs .
	docker run --rm spark-pipeline-labs

check: lint typecheck test  ## what CI runs
