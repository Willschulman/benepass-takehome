def test_get_connection(duckdb_resource):
    """Resource returns a usable DuckDB connection."""
    conn = duckdb_resource.get_connection()
    result = conn.execute("SELECT 42 AS answer").fetchone()
    conn.close()
    assert result[0] == 42


def test_execute(duckdb_resource):
    """Resource can execute DDL and DML."""
    conn = duckdb_resource.get_connection()
    conn.execute("CREATE TABLE test (id INTEGER)")
    conn.execute("INSERT INTO test VALUES (1), (2), (3)")
    count = conn.execute("SELECT count(*) FROM test").fetchone()[0]
    conn.close()
    assert count == 3
