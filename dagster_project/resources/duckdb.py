import duckdb
from dagster import ConfigurableResource


class DuckDBResource(ConfigurableResource):
    """DuckDB connection resource for Dagster assets."""

    db_path: str

    def get_connection(self) -> duckdb.DuckDBPyConnection:
        """Return a connection to the DuckDB database."""
        return duckdb.connect(self.db_path)
