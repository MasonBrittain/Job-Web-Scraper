# Job Market Intelligence Pipeline

[![CI](https://github.com/MasonBrittain/Job-Web-Scraper/actions/workflows/ci.yml/badge.svg)](https://github.com/MasonBrittain/Job-Web-Scraper/actions/workflows/ci.yml)

A small but production-shaped data engineering project: it snapshots tech job
postings every day from public job-board APIs, models them into an analytical
warehouse, and serves a live dashboard of skill demand across companies.

```
                        ┌──────────────────────────────────────────────────┐
                        │                 Dagster (daily 06:00 UTC)        │
                        └──────────────────────────────────────────────────┘
 Greenhouse API  ─┐        ingest              dbt build            serve
 (10 companies)   ├──►  raw parquet   ──►   DuckDB warehouse  ──►  Streamlit
 Lever API       ─┘    (bronze layer)      staging -> marts        dashboard
                       hive-partitioned    + 14 data tests
                       by ingest_date
```

## What it does

Every day the pipeline takes a snapshot of every open posting at ~11 tracked
companies (Stripe, Airbnb, Datadog, Cloudflare, Palantir, ...) via the public
Greenhouse and Lever job-board APIs — no HTML scraping, no auth, and polite
rate limiting. Snapshots accumulate into posting *lifecycles*: when a job first
appeared, when it disappeared, and how long it stayed open. A regex-based skill
taxonomy (a dbt seed — adding a skill is a one-line CSV change) turns raw
descriptions into skill-demand analytics.

## Design decisions

**Bronze layer keeps full fidelity.** Ingestion lands the *entire* upstream
payload as parquet, partitioned `ingest_date=/source=/company=`. Every
downstream table can be rebuilt from history if the modeling logic changes —
the scrape is the only unrepeatable step, so it is the only step that stores
raw data.

**Idempotent by construction.** Re-running a day's ingestion overwrites that
day's partitions instead of appending; dbt models are full rebuilds over all
partitions. Running the pipeline twice produces the same warehouse as running
it once. Backfills are just `ingest(date)` for the missing day.

**The warehouse never ingests raw files.** dbt-duckdb reads the parquet tree
in place (`read_parquet` with hive partitioning) via a dbt *source*, so
staging is a view over the landing zone and there is no copy step to drift.

**Failures degrade, they don't cascade.** Companies migrate ATS vendors, so a
dead board is logged and skipped; the run only fails if *every* board fails.
Board health is surfaced as Dagster asset metadata on each run.

**Tests gate the marts.** `dbt build` interleaves tests with models — 14
checks (uniqueness of the posting key, accepted sources, not-null lifecycle
fields) run as part of every scheduled materialization, not as an
afterthought.

## Stack

| Layer | Tool | Why |
|---|---|---|
| Orchestration | Dagster | asset-based lineage (ingest → staging → marts in one graph), daily schedule, run metadata |
| Ingestion | Python + requests | the sources are JSON APIs; a browser-based scraper would be the wrong tool |
| Storage (bronze) | Parquet | columnar, typed, hive-partitioned; the immutable system of record |
| Warehouse | DuckDB | zero-ops local OLAP; the dbt models port to Snowflake/BigQuery unchanged in spirit |
| Transformation | dbt | staging/marts separation, data tests, seed-driven skill taxonomy |
| Dashboard | Streamlit + Plotly | read-only consumer of the marts; no pipeline logic in the view layer |

## Data model

- `stg_job_postings` — typed, deduplicated view over raw snapshots (one row
  per posting per day observed)
- `fct_job_postings` — one row per unique posting with `first_seen_date`,
  `last_seen_date`, `is_active`, `days_observed`
- `fct_job_skills` — one row per (posting, skill) match from the `skills`
  seed taxonomy

## Running it

```powershell
python -m venv .venv
.venv\Scripts\pip install -r requirements.txt

# one-shot manual run
.venv\Scripts\python -m jobintel.ingestion.run      # snapshot the boards
cd dbt_project; ..\.venv\Scripts\dbt build --profiles-dir .; cd ..

# or let the orchestrator drive it
.venv\Scripts\dagster dev                            # UI at localhost:3000

# dashboard
.venv\Scripts\streamlit run dashboard/app.py
```

