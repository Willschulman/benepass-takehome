from dagster import AssetExecutionContext, MetadataValue, asset

from dagster_project.constants import DATA_DIR
from dagster_project.resources.duckdb import DuckDBResource


def _ingest_csv(context: AssetExecutionContext, duckdb: DuckDBResource, table: str) -> None:
    """Load a CSV file into a DuckDB table in the raw schema."""
    csv_path = DATA_DIR / f"{table}.csv"
    conn = duckdb.get_connection()
    try:
        conn.execute("CREATE SCHEMA IF NOT EXISTS raw")
        conn.execute(
            f"CREATE OR REPLACE TABLE raw.{table} AS SELECT * FROM read_csv_auto('{csv_path}')"
        )
        count = conn.execute(f"SELECT count(*) FROM raw.{table}").fetchone()[0]
    finally:
        conn.close()
    context.add_output_metadata({"row_count": MetadataValue.int(count)})


@asset(key_prefix=["raw"], group_name="raw")
def employees(context: AssetExecutionContext, duckdb: DuckDBResource) -> None:
    """Ingest employees.csv into DuckDB."""
    _ingest_csv(context, duckdb, "employees")


@asset(key_prefix=["raw"], group_name="raw")
def benefit_accounts(context: AssetExecutionContext, duckdb: DuckDBResource) -> None:
    """Ingest benefit_accounts.csv into DuckDB."""
    _ingest_csv(context, duckdb, "benefit_accounts")


@asset(key_prefix=["raw"], group_name="raw")
def transactions(context: AssetExecutionContext, duckdb: DuckDBResource) -> None:
    """Ingest transactions.csv into DuckDB."""
    _ingest_csv(context, duckdb, "transactions")
