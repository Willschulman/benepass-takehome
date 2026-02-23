import duckdb
import pytest

from dagster_project.constants import DUCKDB_PATH


@pytest.fixture
def db():
    """Connect to the dev DuckDB (assumes pipeline has been run)."""
    conn = duckdb.connect(DUCKDB_PATH, read_only=True)
    yield conn
    conn.close()


# ---------------------------------------------------------------------------
# Acceptance tests — validate known sample data issues
# ---------------------------------------------------------------------------

QUARANTINED_TRANSACTIONS = {
    "T90005": "cross_employee_account_usage",
    "T90006": "terminated_employee",
    "T90007": "negative_amount",
}


def test_quarantine_catches_known_issues(db):
    """Quarantine table contains exactly the 3 known bad records."""
    rows = db.execute(
        "SELECT transaction_id, rejection_reason "
        "FROM silver.quarantine_transactions ORDER BY transaction_id"
    ).fetchall()
    reasons = {row[0]: row[1] for row in rows}
    assert reasons == QUARANTINED_TRANSACTIONS


def test_fact_excludes_quarantined(db):
    """No quarantined transaction IDs appear in the fact table."""
    quarantined_ids = set(QUARANTINED_TRANSACTIONS.keys())
    fact_ids = {
        row[0]
        for row in db.execute(
            "SELECT transaction_id FROM gold.fact_transactions"
        ).fetchall()
    }
    assert fact_ids & quarantined_ids == set()


def test_pii_is_masked(db):
    """Employee emails are masked in staging models."""
    emails = [
        row[0]
        for row in db.execute("SELECT email FROM bronze.stg_employees").fetchall()
    ]
    assert all(email.startswith("****@") for email in emails)


# ---------------------------------------------------------------------------
# Structural tests — data-independent pipeline invariants
# ---------------------------------------------------------------------------

def test_staging_preserves_all_raw_rows(db):
    """Staging layer never drops rows — raw count equals staging count."""
    raw = db.execute("SELECT count(*) FROM raw.transactions").fetchone()[0]
    stg = db.execute("SELECT count(*) FROM bronze.stg_transactions").fetchone()[0]
    assert stg == raw


def test_intermediate_preserves_all_staged_rows(db):
    """Intermediate enrichment never drops rows."""
    stg = db.execute("SELECT count(*) FROM bronze.stg_transactions").fetchone()[0]
    inter = db.execute("SELECT count(*) FROM silver.int_transactions_enriched").fetchone()[0]
    assert inter == stg


def test_quarantine_plus_fact_equals_total(db):
    """No records silently dropped — quarantine + fact = intermediate."""
    inter = db.execute("SELECT count(*) FROM silver.int_transactions_enriched").fetchone()[0]
    fact = db.execute("SELECT count(*) FROM gold.fact_transactions").fetchone()[0]
    quarantine = db.execute("SELECT count(*) FROM silver.quarantine_transactions").fetchone()[0]
    assert fact + quarantine == inter


def test_fact_amounts_are_positive(db):
    """All fact table amounts are positive (negatives are quarantined)."""
    negatives = db.execute(
        "SELECT count(*) FROM gold.fact_transactions WHERE amount <= 0"
    ).fetchone()[0]
    assert negatives == 0


def test_fact_dq_flags_are_empty_for_clean_data(db):
    """All surviving transactions have empty dq_flags."""
    flagged = db.execute(
        "SELECT transaction_id FROM gold.fact_transactions WHERE len(dq_flags) > 0"
    ).fetchall()
    assert flagged == []


def test_dim_employees_matches_staging(db):
    """Dimension table has one row per staged employee."""
    stg = db.execute("SELECT count(*) FROM bronze.stg_employees").fetchone()[0]
    dim = db.execute("SELECT count(*) FROM gold.dim_employees").fetchone()[0]
    assert dim == stg


def test_dim_employees_tenure_is_positive(db):
    """All employees have non-negative tenure days."""
    negative = db.execute(
        "SELECT count(*) FROM gold.dim_employees WHERE tenure_days < 0"
    ).fetchone()[0]
    assert negative == 0


def test_dim_benefit_programs_are_distinct(db):
    """Benefit programs dimension has rows and all are distinct."""
    count = db.execute("SELECT count(*) FROM gold.dim_benefit_programs").fetchone()[0]
    assert count > 0
    distinct = db.execute(
        "SELECT count(DISTINCT benefit_type || '|' || program_name) FROM gold.dim_benefit_programs"
    ).fetchone()[0]
    assert count == distinct


def test_dim_merchants_are_distinct(db):
    """Merchants dimension has one row per merchant."""
    count = db.execute("SELECT count(*) FROM gold.dim_merchants").fetchone()[0]
    assert count > 0
    distinct = db.execute(
        "SELECT count(DISTINCT merchant) FROM gold.dim_merchants"
    ).fetchone()[0]
    assert count == distinct
