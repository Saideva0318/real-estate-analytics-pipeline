.PHONY: help install lint format test test-cov run clean

## help: Show available commands
help:
	@echo "Real Estate Analytics Pipeline — Available commands:"
	@sed -n 's/^##//p' $(MAKEFILE_LIST) | column -t -s ':' | sed -e 's/^/ /'

## install: Install Python dependencies
install:
	pip install -r requirements.txt
chore(makefile): add Makefile with install, lint, format, test, run, snowflake-load targets
## lint: Run flake8 linter
lint:
	flake8 src/ tests/ --max-line-length=120 --ignore=E203,W503

## format: Format with Black + isort
format:
	black src/ tests/ --line-length 120
	isort src/ tests/

## test: Run unit tests
test:
	pytest tests/ -v --tb=short

## test-cov: Run tests with coverage
test-cov:
	pytest tests/ -v --cov=src --cov-report=term-missing

## run: Run the full analytics pipeline
run:
	python -m src.main

## snowflake-load: Load data directly to Snowflake
snowflake-load:
	python -m src.snowflake_loader

## clean: Remove cache
clean:
	find . -type d -name __pycache__ -exec rm -rf {} +
	rm -rf .pytest_cache htmlcov .coverage
