from unittest.mock import MagicMock

from dagster_project.checks.data_quality import get_dq_flag_count, get_quarantine_summary


def _mock_duckdb(rows):
    """Create a mock DuckDBResource returning given rows."""
    mock = MagicMock()
    conn = MagicMock()
    mock.get_connection.return_value = conn
    conn.execute.return_value.fetchall.return_value = rows
    conn.execute.return_value.fetchone.return_value = (rows,) if isinstance(rows, int) else rows
    return mock


def test_get_quarantine_summary_empty():
    duckdb = _mock_duckdb([])
    assert get_quarantine_summary(duckdb) == {}


def test_get_quarantine_summary_with_reasons():
    duckdb = _mock_duckdb([("negative_amount", 1), ("terminated_employee", 2)])
    result = get_quarantine_summary(duckdb)
    assert result == {"negative_amount": 1, "terminated_employee": 2}


def test_get_dq_flag_count_zero():
    mock = MagicMock()
    conn = MagicMock()
    mock.get_connection.return_value = conn
    conn.execute.return_value.fetchone.return_value = (0,)
    assert get_dq_flag_count(mock) == 0


def test_get_dq_flag_count_nonzero():
    mock = MagicMock()
    conn = MagicMock()
    mock.get_connection.return_value = conn
    conn.execute.return_value.fetchone.return_value = (3,)
    assert get_dq_flag_count(mock) == 3
