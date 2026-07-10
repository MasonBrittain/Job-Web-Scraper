"""Central configuration: paths and the set of company job boards to track.

Both Greenhouse and Lever expose public, unauthenticated JSON APIs for the
job boards that companies host with them. Companies migrate ATS vendors over
time, so a token that stops resolving is logged and skipped rather than
failing the run.
"""
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw" / "job_postings"
WAREHOUSE_DIR = DATA_DIR / "warehouse"
DUCKDB_PATH = WAREHOUSE_DIR / "jobintel.duckdb"
DBT_PROJECT_DIR = PROJECT_ROOT / "dbt_project"

# guarantee the data tree exists so a fresh clone runs without setup steps
RAW_DIR.mkdir(parents=True, exist_ok=True)
WAREHOUSE_DIR.mkdir(parents=True, exist_ok=True)

# board tokens for https://boards-api.greenhouse.io/v1/boards/{token}/jobs
GREENHOUSE_BOARDS = [
    "stripe",
    "airbnb",
    "dropbox",
    "datadog",
    "gitlab",
    "figma",
    "duolingo",
    "coinbase",
    "cloudflare",
    "robinhood",
]

# site tokens for https://api.lever.co/v0/postings/{token}?mode=json
LEVER_SITES = [
    "palantir",
]

REQUEST_TIMEOUT_SECONDS = 30
# polite delay between requests to the same API host
REQUEST_DELAY_SECONDS = 1.0
