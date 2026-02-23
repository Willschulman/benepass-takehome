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
        row_count = conn.execute(f"SELECT count(*) FROM raw.{table}").fetchone()[0]
        schema_rows = conn.execute(f"DESCRIBE raw.{table}").fetchall()
        schema_snapshot = {row[0]: row[1] for row in schema_rows}
        null_rates = {}
        for col in schema_snapshot:
            null_count = conn.execute(
                f'SELECT count(*) FROM raw.{table} WHERE "{col}" IS NULL'
            ).fetchone()[0]
            null_rates[col] = round(null_count / row_count, 4) if row_count > 0 else 0.0
        total_cells = row_count * len(schema_snapshot)
        total_nulls = sum(
            conn.execute(
                f'SELECT count(*) FROM raw.{table} WHERE "{col}" IS NULL'
            ).fetchone()[0]
            for col in schema_snapshot
        )
        quality_score = round(1 - (total_nulls / total_cells), 4) if total_cells > 0 else 1.0
    finally:
        conn.close()
    context.add_output_metadata({
        "row_count": MetadataValue.int(row_count),
        "column_count": MetadataValue.int(len(schema_snapshot)),
        "schema": MetadataValue.json(schema_snapshot),
        "null_rates": MetadataValue.json(null_rates),
        "quality_score": MetadataValue.float(quality_score),
    })


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
