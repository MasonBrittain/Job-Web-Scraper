"""Bronze-layer landing zone: raw postings as hive-partitioned parquet.

Layout:
    data/raw/job_postings/ingest_date=YYYY-MM-DD/source=<source>/company=<token>/data.parquet

Partitioning by ingest date makes the pipeline idempotent: re-running a day
overwrites that day's files instead of appending duplicates, and dbt reads
the whole tree with hive partitioning so partition keys become columns.
"""
from __future__ import annotations

import logging
from datetime import date

import pandas as pd

from jobintel.config import RAW_DIR

logger = logging.getLogger(__name__)

COLUMNS = [
    "source",
    "company",
    "job_id",
    "title",
    "department",
    "location",
    "url",
    "published_at",
    "description_text",
    "raw_payload",
    "ingested_at",
]


def write_partition(records: list[dict], source: str, company: str, ingest_date: date) -> str:
    """Write one company's postings for one ingest date, overwriting any prior run."""
    part_dir = (
        RAW_DIR
        / f"ingest_date={ingest_date.isoformat()}"
        / f"source={source}"
        / f"company={company}"
    )
    part_dir.mkdir(parents=True, exist_ok=True)
    path = part_dir / "data.parquet"

    # partition keys live in the path, not the file
    frame = pd.DataFrame(records, columns=COLUMNS).drop(columns=["source", "company"])
    frame.to_parquet(path, index=False)
    logger.info("Wrote %d rows -> %s", len(frame), path)
    return str(path)
