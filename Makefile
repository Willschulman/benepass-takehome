.PHONY: setup test dev clean dbt-build

setup:
	uv sync --all-groups

test:
	uv run pytest tests/ -v

dev:
	uv run dagster dev

dbt-build:
	cd dbt_project && uv run dbt build

clean:
	rm -f data/benepass.duckdb
	rm -rf dbt_project/target/
	rm -rf dbt_project/dbt_packages/
