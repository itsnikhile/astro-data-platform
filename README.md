# Astro + dbt Demo Pipeline

A production-style data pipeline built with **Astronomer (Astro)**, **Apache Airflow**, and **dbt Core** using **Astronomer Cosmos** for dbt-native orchestration.

## Architecture

```
[Raw Ingestion DAG]  ──triggers──►  [dbt Transformations DAG]
       │                                        │
  raw.events                         staging.stg_events
  (Postgres)                         marts.fct_events_incremental
                                     marts.fct_daily_event_summary
```

**Stack:**
- **Astronomer Cloud (Astro)** — managed Airflow platform
- **Astro CLI** — local dev & CI/CD deployments
- **Apache Airflow 2.8** — orchestration
- **Astronomer Cosmos** — dbt ↔ Airflow integration (each dbt node = Airflow task)
- **dbt Core 1.7** — transformations with incremental models & data quality tests
- **Postgres** — warehouse (swap in Snowflake/BigQuery/Redshift for production)

---

## Project Structure

```
astro-data-platform/
├── dags/
│   ├── data_ingestion_dag.py          # Ingest raw events → trigger dbt
│   └── dbt_transformations_dag.py     # Cosmos DbtDag — all dbt models
├── include/
│   └── dbt/
│       ├── dbt_project.yml
│       ├── packages.yml
│       ├── profiles/profiles.yml
│       └── models/
│           ├── staging/
│           │   ├── stg_events.sql     # Clean & deduplicate raw events
│           │   └── schema.yml         # Source freshness + column tests
│           └── marts/
│               ├── fct_events_incremental.sql   # Incremental merge model
│               ├── fct_daily_event_summary.sql  # Daily aggregation
│               └── schema.yml
├── tests/
│   └── test_dags.py                   # Airflow DAG unit tests (pytest)
├── .github/workflows/ci_cd.yml        # GitHub Actions: lint → test → deploy
├── Dockerfile
├── requirements.txt
├── packages.txt
└── .env.example
```

---

## Local Development

### Prerequisites
- [Docker Desktop](https://www.docker.com/products/docker-desktop/)
- [Astro CLI](https://docs.astronomer.io/astro/cli/install-cli)
- Python 3.9+

### Quick Start

```bash
# 1. Clone the repo
git clone https://github.com/your-username/astro-data-platform.git
cd astro-data-platform

# 2. Copy and configure environment variables
cp .env.example .env

# 3. Start the local Astro environment (Airflow + Postgres via Docker)
astro dev start

# 4. Open Airflow UI
open http://localhost:8080
# Username: admin | Password: admin

# 5. Install dbt packages
astro dev run dbt deps --project-dir include/dbt --profiles-dir include/dbt/profiles

# 6. Compile dbt models to verify SQL
astro dev run dbt compile --project-dir include/dbt --profiles-dir include/dbt/profiles

# 7. Run unit tests
pytest tests/ -v
```

### Trigger the Pipeline Manually

```bash
# Trigger ingestion (which auto-triggers dbt)
astro dev run airflow dags trigger data_ingestion_dag

# Or trigger dbt only
astro dev run airflow dags trigger dbt_transformations_dag
```

---

## dbt Models

| Model | Materialization | Description |
|---|---|---|
| `stg_events` | View | Cleans and deduplicates raw events |
| `fct_events_incremental` | Incremental (merge) | Enriched events — only processes new rows |
| `fct_daily_event_summary` | Table | Daily per-user event aggregations |

### Incremental Strategy

`fct_events_incremental` uses `incremental_strategy='merge'` with `unique_key='event_id'`:

- **New records** are inserted
- **Updated records** (same `event_id`) are merged/overwritten
- **Schema changes** are handled with `on_schema_change='append_new_columns'`

---

## Data Quality

Tests run automatically after every dbt model via Cosmos:

- **Schema tests** — `unique`, `not_null`, `accepted_values` on all key columns
- **Source freshness** — warns if raw data is >1hr stale, errors at 4hrs
- **Custom singular tests** — `assert_no_negative_revenue.sql`
- **Expression tests** — no future-dated events, non-negative counts

---

## CI/CD

GitHub Actions pipeline on every push to `main`:

1. **Lint** — Ruff lints all DAG files
2. **Unit tests** — pytest validates DAG structure and imports
3. **dbt compile** — verifies all SQL compiles without errors
4. **Deploy** — `astro deploy` pushes to Astronomer Cloud (main branch only)

### Required GitHub Secrets

| Secret | Description |
|---|---|
| `ASTRONOMER_KEY_ID` | Astronomer Cloud API key ID |
| `ASTRONOMER_KEY_SECRET` | Astronomer Cloud API key secret |
| `ASTRO_DEPLOYMENT_ID` | Target Deployment ID in Astronomer Cloud |

---

## Deployment to Astronomer Cloud

```bash
# Authenticate
astro login

# Create a new deployment (first time)
astro deployment create

# Deploy
astro deploy <deployment-id>
```

Set the following environment variables in the Astronomer Cloud UI under your Deployment settings:
- `DBT_HOST`, `DBT_PORT`, `DBT_USER`, `DBT_PASSWORD`, `DBT_DATABASE`, `DBT_SCHEMA`
- Airflow Connection: `postgres_default`
