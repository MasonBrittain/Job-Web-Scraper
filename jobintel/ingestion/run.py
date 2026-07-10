"""Ad-hoc ingestion entry point: ``python -m jobintel.ingestion.run``.

Dagster drives the same function on a schedule; this module exists so the
ingest step can be run or backfilled by hand without the orchestrator.
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timezone

from jobintel.config import GREENHOUSE_BOARDS, LEVER_SITES
from jobintel.ingestion.landing import write_partition
from jobintel.ingestion.sources import fetch_all


def ingest(ingest_date: date | None = None) -> dict:
    """Fetch every configured board and land it as parquet.

    Returns a summary dict (row counts per board, failures) that Dagster
    surfaces as asset metadata.
    """
    ingest_date = ingest_date or datetime.now(timezone.utc).date()
    boards = {"greenhouse": GREENHOUSE_BOARDS, "lever": LEVER_SITES}
    results, failures = fetch_all(boards)

    counts = {}
    for key, records in results.items():
        if not records:
            # an empty board is usually a dead token, not a company with no
            # openings — surface it as a failure so it gets pruned from config
            failures.append(f"{key} (empty board)")
            continue
        source, company = key.split("/", 1)
        write_partition(records, source, company, ingest_date)
        counts[key] = len(records)

    summary = {
        "ingest_date": ingest_date.isoformat(),
        "boards_ok": len(counts),
        "boards_failed": failures,
        "total_rows": sum(counts.values()),
        "rows_per_board": counts,
    }
    return summary


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    print(ingest())
