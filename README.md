# Benepass Data Engineering Take-Home

A lightweight ELT pipeline that ingests 3 CSVs (employees, benefit accounts, transactions), transforms them into analytics-ready tables, and surfaces data quality issues, orchestrated by Dagster with dbt as the transformation layer and DuckDB as the analytical query engine.

**Live demo:** [benepass.wschulman.com](https://benepass.wschulman.com) (credentials sent separately)

| Link | Description |
|------|-------------|
| [Dashboard](https://benepass.wschulman.com) | Evidence analytics dashboard |
| [Dagster UI](https://dagster.benepass.wschulman.com) | Pipeline orchestration and asset lineage |
| [dbt Docs](https://docs.benepass.wschulman.com) | Column-level documentation, lineage, and domain tags |

## How to Run

**Prerequisites:** Python 3.13+, [uv](https://docs.astral.sh/uv/)

```bash
git clone https://github.com/Willschulman/benepass-takehome.git
cd benepass-takehome
uv sync
```

Start the Dagster UI:

```bash
uv run dagster dev
```

Open http://localhost:3000 and click **Materialize all** to run the full pipeline (ingestion → dbt staging → intermediate → marts + quarantine).

Run the test suite:

```bash
uv run pytest tests/ -v              # unit + structural tests
cd dbt_project && uv run dbt test    # schema validation tests
```

## Assumptions

- **Flag, don't fail.** The pipeline never halts on bad data. The business gets visibility into issues through quarantine tables and DQ flags rather than pipeline failures that block downstream consumers.
- **Quarantine is exclusive.** A record lands in either `fact_transactions` or `quarantine_transactions`, never both. The most severe issue is recorded as the rejection reason.
- **PII masking is on by default.** Emails are masked with `****@domain.com` in all environments. A `pii_masking_enabled` dbt variable controls this behavior.
- **Static source data.** The CSVs are treated as a one-time full load. Incremental loading patterns are discussed in the Production Architecture section below.
- **DuckDB is the right engine for this scale.** Embedded, zero-config, fast for analytical queries on small datasets. Production alternatives are discussed below.

## Key Design Decisions

- **Dagster + dbt native integration.** `dagster-dbt` maps every dbt model to a Dagster asset automatically. The Dagster UI shows the full dependency graph from CSV ingestion through marts, and asset checks (schema drift, DQ) attach directly to the assets they validate.
- **Two-tier data quality.** Tier 1 flags minor issues (category mismatches) in a `dq_flags` array on `fact_transactions`. Tier 2 quarantines serious problems (negative amounts, terminated employees, broken FKs) into a separate `quarantine_transactions` table. Analysts get clean data by default with full auditability.
- **DQ logic in SQL, not Python.** All quarantine and flagging logic lives in dbt models. This keeps it auditable, testable with `dbt test`, and readable by anyone who knows SQL.
- **Metadata at two layers.** Dagster captures runtime metadata per materialization (row counts, schema snapshots, null rates, quality scores). dbt captures structural metadata (column descriptions, domain tags, PII classification). Together: "what happened on this run" + "what does this data mean."
- **Structural + acceptance tests.** Unit tests verify pipeline invariants that hold regardless of data (staging never drops rows, quarantine + fact = total). Acceptance tests validate the known sample data issues (specific transactions quarantined for specific reasons). Schema drift checks catch structural problems before dbt runs.
- **CI/CD with GitHub Actions.** Every push runs lint, pytest, and `dbt parse`. Pushes to main auto-deploy via SSH. In production this would be Terraform-managed infrastructure with environment specific deployments. 

## Production Architecture (AWS)

This take-home demonstrates the patterns at small scale. In production on AWS, the same architecture extends naturally:

- **Storage.** S3 + Apache Iceberg as the raw storage layer. Data lives in open Parquet files, cataloged in AWS Glue, queryable by any engine. The warehouse (Snowflake, Redshift, or Databricks) reads Iceberg natively, making the warehouse choice reversible.
- **Ingestion.** Dagster orchestrates all sources. CDC from Aurora Postgres via Debezium Server writing directly to Iceberg on S3. SaaS and API sources via dlt (lightweight Python library that runs inside Dagster assets, no separate infrastructure). Heavier workloads like backfills or complex multi-source joins via AWS Glue jobs, still orchestrated as Dagster assets.
- **Transformation.** Same dbt project with layered modeling (bronze/silver/gold). Incremental models introduced where full refreshes become impractical. PII masking tied to warehouse-level role-based policies rather than a dbt variable.
- **Deployment.** Terragrunt-managed infrastructure for environment isolation (dev/staging/prod share the same Terraform modules with environment-specific configs). Dagster deployment model depends on the team's existing infrastructure (managed Dagster Cloud, self-hosted on K8s, or ECS). Branch deployments backed by warehouse zero-copy clones give every PR an isolated environment running `dbt build` and tests against real data shapes before merge.
- **Observability.** Same patterns from this project (schema drift checks, quarantine monitoring, DQ flags) extended with Dagster alerting to Slack, freshness policies on gold models, and volume anomaly detection.
