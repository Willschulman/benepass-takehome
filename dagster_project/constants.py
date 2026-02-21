from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data"
DBT_PROJECT_DIR = PROJECT_ROOT / "dbt_project"
DUCKDB_PATH = str(DATA_DIR / "benepass.duckdb")

EXPECTED_SCHEMAS: dict[str, dict[str, str]] = {
    "employees": {
        "employee_id": "VARCHAR",
        "first_name": "VARCHAR",
        "last_name": "VARCHAR",
        "email": "VARCHAR",
        "hire_date": "DATE",
        "status": "VARCHAR",
    },
    "benefit_accounts": {
        "benefit_account_id": "VARCHAR",
        "employee_id": "VARCHAR",
        "benefit_type": "VARCHAR",
        "program_name": "VARCHAR",
        "monthly_allowance": "BIGINT",
        "current_balance": "DOUBLE",
    },
    "transactions": {
        "transaction_id": "VARCHAR",
        "employee_id": "VARCHAR",
        "benefit_account_id": "VARCHAR",
        "merchant": "VARCHAR",
        "amount": "DOUBLE",
        "timestamp": "TIMESTAMP WITH TIME ZONE",
        "category": "VARCHAR",
    },
}
