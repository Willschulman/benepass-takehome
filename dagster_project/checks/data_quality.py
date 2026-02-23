from dagster import AssetCheckResult, AssetCheckSeverity, AssetKey, asset_check

from dagster_project.resources.duckdb import DuckDBResource


def get_quarantine_summary(duckdb: DuckDBResource) -> dict[str, int]:
    """Query quarantine table for record counts by rejection reason."""
    conn = duckdb.get_connection()
    try:
        rows = conn.execute(
            "SELECT rejection_reason, count(*) as cnt "
            "FROM silver.quarantine_transactions "
            "GROUP BY rejection_reason"
        ).fetchall()
    finally:
        conn.close()
    return {row[0]: row[1] for row in rows}


def get_dq_flag_count(duckdb: DuckDBResource) -> int:
    """Count records in fact_transactions with non-empty dq_flags."""
    conn = duckdb.get_connection()
    try:
        result = conn.execute(
            "SELECT count(*) FROM gold.fact_transactions WHERE len(dq_flags) > 0"
        ).fetchone()
    finally:
        conn.close()
    return result[0]


@asset_check(
    asset=AssetKey(["quarantine_transactions"]),
    description="Warn when records are quarantined due to data quality issues",
)
def quarantine_records_check(duckdb: DuckDBResource) -> AssetCheckResult:
    """Warn if any transactions were quarantined."""
    summary = get_quarantine_summary(duckdb)
    total = sum(summary.values())
    return AssetCheckResult(
        passed=total == 0,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "quarantined_records": total,
            "by_reason": summary,
        },
    )


@asset_check(
    asset=AssetKey(["fact_transactions"]),
    description="Warn when clean records have data quality flags",
)
def dq_flags_check(duckdb: DuckDBResource) -> AssetCheckResult:
    """Warn if any fact transactions carry DQ flags."""
    flagged = get_dq_flag_count(duckdb)
    return AssetCheckResult(
        passed=flagged == 0,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "flagged_records": flagged,
        },
    )
