import duckdb
import pytest

from dagster_project.constants import DUCKDB_PATH

QUARANTINED_TRANSACTIONS = {
    "T90005": "cross_employee_account_usage",
    "T90006": "terminated_employee",
    "T90007": "negative_amount",
}

CLEAN_TRANSACTION_IDS = ["T90001", "T90002", "T90003", "T90004"]


@pytest.fixture
def db():
    """Connect to the dev DuckDB (assumes pipeline has been run)."""
    conn = duckdb.connect(DUCKDB_PATH, read_only=True)
    yield conn
    conn.close()


def test_quarantine_catches_known_issues(db):
    """Quarantine table contains exactly the 3 known bad records."""
    rows = db.execute(
        "SELECT transaction_id, rejection_reason "
        "FROM silver.quarantine_transactions ORDER BY transaction_id"
    ).fetchall()
    reasons = {row[0]: row[1] for row in rows}
    assert reasons == QUARANTINED_TRANSACTIONS


def test_fact_excludes_quarantined(db):
    """Fact table excludes all quarantined records."""
    fact_ids = [
        row[0]
        for row in db.execute(
            "SELECT transaction_id FROM gold.fact_transactions ORDER BY transaction_id"
        ).fetchall()
    ]
    assert fact_ids == CLEAN_TRANSACTION_IDS


def test_fact_dq_flags_are_empty_for_clean_data(db):
    """All surviving transactions have empty dq_flags (categories match benefit types)."""
    flagged = db.execute(
        "SELECT transaction_id, dq_flags FROM gold.fact_transactions WHERE len(dq_flags) > 0"
    ).fetchall()
    assert flagged == []


def test_pii_is_masked(db):
    """Employee emails are masked in staging models."""
    emails = [
        row[0]
        for row in db.execute("SELECT email FROM bronze.stg_employees").fetchall()
    ]
    assert all(email.startswith("****@") for email in emails)


def test_staging_passes_all_records_through(db):
    """Staging layer does not filter — all 7 transactions are present."""
    count = db.execute("SELECT count(*) FROM bronze.stg_transactions").fetchone()[0]
    assert count == 7


def test_intermediate_enriches_all_transactions(db):
    """Intermediate layer enriches all 7 transactions with employee/account data."""
    count = db.execute("SELECT count(*) FROM silver.int_transactions_enriched").fetchone()[0]
    assert count == 7
