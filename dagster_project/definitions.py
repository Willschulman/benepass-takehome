from dagster import Definitions, in_process_executor
from dagster_dbt import DbtCliResource

from dagster_project.assets.dbt import benepass_dbt_assets, dbt_project
from dagster_project.assets.ingestion import benefit_accounts, employees, transactions
from dagster_project.checks.schema_drift import (
    benefit_accounts_extra_columns_check,
    benefit_accounts_schema_check,
    employees_extra_columns_check,
    employees_schema_check,
    transactions_extra_columns_check,
    transactions_schema_check,
)
from dagster_project.constants import DUCKDB_PATH
from dagster_project.resources.duckdb import DuckDBResource

defs = Definitions(
    assets=[
        employees,
        benefit_accounts,
        transactions,
        benepass_dbt_assets,
    ],
    asset_checks=[
        employees_schema_check,
        employees_extra_columns_check,
        benefit_accounts_schema_check,
        benefit_accounts_extra_columns_check,
        transactions_schema_check,
        transactions_extra_columns_check,
    ],
    resources={
        "duckdb": DuckDBResource(db_path=DUCKDB_PATH),
        "dbt": DbtCliResource(project_dir=dbt_project.project_dir),
    },
    executor=in_process_executor,
)
