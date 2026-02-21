from dagster import materialize

from dagster_project.assets.ingestion import benefit_accounts, employees, transactions
from dagster_project.resources.duckdb import DuckDBResource


def test_raw_employees(duckdb_resource: DuckDBResource) -> None:
    """Ingests employees.csv into DuckDB."""
    result = materialize(
        [employees],
        resources={"duckdb": duckdb_resource},
    )
    assert result.success
    conn = duckdb_resource.get_connection()
    count = conn.execute("SELECT count(*) FROM raw.employees").fetchone()[0]
    assert count == 4
    conn.close()


def test_raw_benefit_accounts(duckdb_resource: DuckDBResource) -> None:
    """Ingests benefit_accounts.csv into DuckDB."""
    result = materialize(
        [benefit_accounts],
        resources={"duckdb": duckdb_resource},
    )
    assert result.success
    conn = duckdb_resource.get_connection()
    count = conn.execute("SELECT count(*) FROM raw.benefit_accounts").fetchone()[0]
    assert count == 5
    conn.close()


def test_raw_transactions(duckdb_resource: DuckDBResource) -> None:
    """Ingests transactions.csv into DuckDB."""
    result = materialize(
        [transactions],
        resources={"duckdb": duckdb_resource},
    )
    assert result.success
    conn = duckdb_resource.get_connection()
    count = conn.execute("SELECT count(*) FROM raw.transactions").fetchone()[0]
    assert count == 7
    conn.close()
