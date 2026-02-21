import pytest

from dagster_project.resources.duckdb import DuckDBResource


@pytest.fixture
def duckdb_resource(tmp_path) -> DuckDBResource:
    """Provide a temp DuckDB for testing."""
    return DuckDBResource(db_path=str(tmp_path / "test.duckdb"))
