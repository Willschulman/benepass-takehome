from dagster import AssetCheckResult, AssetCheckSeverity, asset_check

from dagster_project.assets.ingestion import benefit_accounts, employees, transactions
from dagster_project.constants import EXPECTED_SCHEMAS
from dagster_project.resources.duckdb import DuckDBResource


def check_missing_columns(
    actual: dict[str, str], expected: dict[str, str]
) -> list[str]:
    """Return list of expected column names not present in actual."""
    return sorted(set(expected) - set(actual))


def check_type_mismatches(
    actual: dict[str, str], expected: dict[str, str]
) -> dict[str, dict[str, str]]:
    """Return dict of columns where actual type differs from expected."""
    mismatches = {}
    for col, expected_type in expected.items():
        if col in actual and actual[col] != expected_type:
            mismatches[col] = {"expected": expected_type, "actual": actual[col]}
    return mismatches


def check_extra_columns(
    actual: dict[str, str], expected: dict[str, str]
) -> list[str]:
    """Return list of column names in actual but not in expected."""
    return sorted(set(actual) - set(expected))


def _get_actual_schema(table_name: str, duckdb: DuckDBResource) -> dict[str, str]:
    """Fetch column name -> type mapping from DuckDB table in raw schema."""
    conn = duckdb.get_connection()
    try:
        rows = conn.execute(f"DESCRIBE raw.{table_name}").fetchall()
    finally:
        conn.close()
    return {row[0]: row[1] for row in rows}


def _build_blocking_check(table_name: str, duckdb: DuckDBResource) -> AssetCheckResult:
    """Tier 1: Fail on missing columns or type mismatches."""
    actual = _get_actual_schema(table_name, duckdb)
    expected = EXPECTED_SCHEMAS[table_name]
    missing = check_missing_columns(actual, expected)
    type_mismatches = check_type_mismatches(actual, expected)
    passed = not missing and not type_mismatches
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "missing_columns": missing,
            "type_mismatches": type_mismatches,
        },
    )


def _build_warning_check(table_name: str, duckdb: DuckDBResource) -> AssetCheckResult:
    """Tier 2: Warn on extra/unexpected columns."""
    actual = _get_actual_schema(table_name, duckdb)
    expected = EXPECTED_SCHEMAS[table_name]
    extra = check_extra_columns(actual, expected)
    passed = not extra
    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.WARN,
        metadata={
            "extra_columns": extra,
        },
    )


@asset_check(asset=employees, blocking=True, description="Validate employees schema (missing columns, type mismatches)")
def employees_schema_check(duckdb: DuckDBResource) -> AssetCheckResult:
    """Blocking check: employees table has all expected columns with correct types."""
    return _build_blocking_check("employees", duckdb)


@asset_check(asset=employees, description="Detect unexpected columns in employees table")
def employees_extra_columns_check(duckdb: DuckDBResource) -> AssetCheckResult:
    """Warning check: employees table has no unexpected columns."""
    return _build_warning_check("employees", duckdb)


@asset_check(asset=benefit_accounts, blocking=True, description="Validate benefit_accounts schema (missing columns, type mismatches)")
def benefit_accounts_schema_check(duckdb: DuckDBResource) -> AssetCheckResult:
    """Blocking check: benefit_accounts table has all expected columns with correct types."""
    return _build_blocking_check("benefit_accounts", duckdb)


@asset_check(asset=benefit_accounts, description="Detect unexpected columns in benefit_accounts table")
def benefit_accounts_extra_columns_check(duckdb: DuckDBResource) -> AssetCheckResult:
    """Warning check: benefit_accounts table has no unexpected columns."""
    return _build_warning_check("benefit_accounts", duckdb)


@asset_check(asset=transactions, blocking=True, description="Validate transactions schema (missing columns, type mismatches)")
def transactions_schema_check(duckdb: DuckDBResource) -> AssetCheckResult:
    """Blocking check: transactions table has all expected columns with correct types."""
    return _build_blocking_check("transactions", duckdb)


@asset_check(asset=transactions, description="Detect unexpected columns in transactions table")
def transactions_extra_columns_check(duckdb: DuckDBResource) -> AssetCheckResult:
    """Warning check: transactions table has no unexpected columns."""
    return _build_warning_check("transactions", duckdb)
