## Coding Conventions
DO NOT ADD ANY COMMENTS TO THE CODE UNLESS EXPLICITLY INSTRUCTED TO

Every function gets a concise one-liner docstring. No inline comments.

```python
def flag_category_mismatch(benefit_type: str, category: str) -> bool:
    """Check if transaction category aligns with its benefit type."""
    return category not in VALID_CATEGORY_MAPPINGS.get(benefit_type, set())
```

Clean, PYTHONIC, not verbose. Engineer maintainable code. Choose tidy.

### Type Hints
Type hints on every function signature and return type. No exceptions.

### No Protocols / No ABCs
This is a single-pipeline project, not a framework. No abstract interfaces. Write clean, well-typed functions and classes directly.

### Pydantic for Boundaries
Pydantic models for config, schema contracts, and validation boundaries. Not for internal data passing — use dataclasses or plain dicts where Pydantic is overkill.

### Pure Functions for Logic
All cleaning, validation, and flagging logic as pure functions. No side effects, no database calls. Easy to test, easy to read.

### Constants and Mappings in Dedicated Modules
Valid category-to-benefit mappings, expected schemas, severity tiers, known business rules — all in their own module(s), not inline.

### Small Focused Modules
Each file should be understandable in isolation. A reviewer opens one file, reads it top to bottom, and gets it.

## What is this?
A take-home data engineering challenge for Benepass. Build a lightweight ELT pipeline that ingests 3 CSVs (employees, benefit_accounts, transactions), transforms them into analytics-ready tables, and surfaces data quality issues — all orchestrated by Dagster with dbt as the transformation layer and DuckDB as the analytical engine.

## Architecture

### Stack
- Python 3.13, uv
- Dagster (orchestration, asset checks, metadata)
- dbt-duckdb (transformation layer)
- DuckDB (analytical engine)
- Evidence (visualization layer)
- Caddy (reverse proxy, auto-HTTPS)
- Docker Compose (deployment)
- Deployed on Hetzner VPS

### Pipeline Flow
```
Raw CSVs
  → Dagster assets (ingest to DuckDB raw tables)
  → Schema drift checks (Dagster asset checks, pre-dbt)
  → dbt staging models (stg_employees, stg_benefit_accounts, stg_transactions)
  → dbt tests (not_null, unique, relationships, accepted_values, custom)
  → dbt mart models (fact_transactions, dim_employees, dim_benefit_programs, dim_merchants)
  → Quarantine table (tier 2 issues separated out)
  → Evidence dashboard (analytics + DQ scorecard)
```

### Testing / Data Quality Split
- **Dagster asset checks**: pipeline health — schema drift detection, row counts, source freshness, schema contracts on raw inputs
- **dbt tests**: data quality — nulls, FK integrity, accepted values, custom business rules
- **dbt models**: quarantine/flagging logic as SQL transformations

### Data Quality Strategy: Flag, Don't Fail
Pipeline never fails on bad data. Two tiers:
- **Tier 1 (Warn + Tag)**: Record flows through, gets flagged in `dq_flags` column on fact table. Category mismatches, unusual amounts.
- **Tier 2 (Quarantine)**: Record has real problem — lands in `quarantine_transactions`, excluded from `fact_transactions`. Terminated employee transactions, broken FKs, negative amounts.

### Schema Drift Detection
Dagster asset checks validate incoming CSV schemas against expected contracts before dbt runs. Catches missing columns, unexpected columns, type drift. Runs on raw ingestion assets as first line of defense.

### Metadata Management
Two complementary layers:
- **dbt**: column-level descriptions, PII classification (`email` field), domain tagging, data contracts via schema.yml
- **Dagster**: runtime metadata per materialization — row counts, schema snapshots, null rates, quality scores

### Known Data Issues to Flag
The source data has intentional problems:
- T90007: negative amount (-$25.00) — refund or error
- T90005: Whole Foods/groceries charged to wellness benefit (BA1001), and BA1001 belongs to E001 but transaction is from E003
- T90006: transaction from terminated employee E004
- BA1004: benefit account exists for E003 (wellness) but has no matching benefit_account_id tie to E003's transactions correctly — E004 is terminated but has a benefit account
- Category/benefit_type mismatches (groceries ≠ wellness, etc.)

### Models

**Staging**
- `stg_employees`: normalized hire_date, validated status values
- `stg_benefit_accounts`: joined employee status, validated benefit types
- `stg_transactions`: normalized timestamps to UTC, joined with benefit account + employee metadata, flagged issues

**Dimensions**
- `dim_employees`: employee attributes, status, tenure
- `dim_benefit_programs`: benefit types, program names, allowances
- `dim_merchants`: distinct merchants with inferred categories

**Facts**
- `fact_transactions`: clean transactions with dq_flags array, only non-quarantined records
- `quarantine_transactions`: tier 2 issues with rejection reason

### Deployment
- Hetzner VPS with Docker Compose (Dagster + DuckDB + Evidence + Caddy)
- Caddy auto-provisions HTTPS via Let's Encrypt
- Dagster UI exposed publicly for reviewer exploration
- Evidence dashboard on separate subdomain

### Project Structure
```
benepass-takehome/
├── dagster_project/
│   ├── assets/
│   │   └── ingestion.py
│   ├── checks/
│   │   └── schema_drift.py
│   ├── resources/
│   │   └── duckdb.py
│   └── definitions.py
├── dbt_project/
│   ├── models/
│   │   ├── staging/
│   │   ├── marts/
│   │   └── quality/
│   ├── tests/
│   ├── macros/
│   └── schema.yml
├── evidence_project/
├── data/
│   ├── employees.csv
│   ├── benefit_accounts.csv
│   └── transactions.csv
├── docker-compose.yml
├── Caddyfile
├── README.md
└── claude.md
```
