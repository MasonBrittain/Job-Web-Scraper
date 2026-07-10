"""Clients for the public Greenhouse and Lever job-board APIs.

Every posting is normalized to a common record shape at the edge, but the
full upstream payload is preserved in ``raw_payload`` so the landing zone
keeps bronze-layer fidelity: downstream models can always be rebuilt from
what the API actually returned.
"""
from __future__ import annotations

import html
import json
import logging
import re
import time
from datetime import datetime, timezone

import requests

from jobintel.config import REQUEST_DELAY_SECONDS, REQUEST_TIMEOUT_SECONDS

logger = logging.getLogger(__name__)

GREENHOUSE_URL = "https://boards-api.greenhouse.io/v1/boards/{token}/jobs?content=true"
LEVER_URL = "https://api.lever.co/v0/postings/{token}?mode=json"

USER_AGENT = "jobintel-pipeline/0.1 (portfolio data-engineering project)"

_TAG_RE = re.compile(r"<[^>]+>")
_WS_RE = re.compile(r"\s+")


def _strip_html(content: str) -> str:
    """Reduce an HTML fragment to plain text good enough for keyword search."""
    text = html.unescape(content or "")
    text = _TAG_RE.sub(" ", text)
    return _WS_RE.sub(" ", text).strip()


def _get_json(url: str) -> dict | list | None:
    """Fetch a JSON document, returning None on any HTTP or parse failure."""
    try:
        resp = requests.get(url, timeout=REQUEST_TIMEOUT_SECONDS, headers={"User-Agent": USER_AGENT})
        if resp.status_code == 404:
            logger.warning("Board not found (company may have changed ATS): %s", url)
            return None
        resp.raise_for_status()
        return resp.json()
    except (requests.RequestException, ValueError) as exc:
        logger.warning("Failed to fetch %s: %s", url, exc)
        return None


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def fetch_greenhouse(board_token: str) -> list[dict] | None:
    """Fetch and normalize all postings for one Greenhouse board.

    Returns None when the board itself is unreachable (vs. an empty list for
    a board that exists but has no postings) so callers can distinguish
    "source broken" from "no data".
    """
    payload = _get_json(GREENHOUSE_URL.format(token=board_token))
    if payload is None or "jobs" not in payload:
        return None

    ingested_at = _now_iso()
    records = []
    for job in payload["jobs"]:
        # Greenhouse double-escapes the HTML body of `content`
        description = _strip_html(html.unescape(job.get("content") or ""))
        departments = [d.get("name") for d in job.get("departments") or [] if d.get("name")]
        records.append(
            {
                "source": "greenhouse",
                "company": board_token,
                "job_id": str(job["id"]),
                "title": job.get("title"),
                "department": departments[0] if departments else None,
                "location": (job.get("location") or {}).get("name"),
                "url": job.get("absolute_url"),
                "published_at": job.get("first_published") or job.get("updated_at"),
                "description_text": description,
                "raw_payload": json.dumps(job),
                "ingested_at": ingested_at,
            }
        )
    return records


def fetch_lever(site_token: str) -> list[dict] | None:
    """Fetch and normalize all postings for one Lever site."""
    payload = _get_json(LEVER_URL.format(token=site_token))
    if payload is None or not isinstance(payload, list):
        return None

    ingested_at = _now_iso()
    records = []
    for job in payload:
        categories = job.get("categories") or {}
        created_ms = job.get("createdAt")
        published_at = (
            datetime.fromtimestamp(created_ms / 1000, tz=timezone.utc).isoformat()
            if created_ms
            else None
        )
        description = job.get("descriptionPlain") or _strip_html(job.get("description") or "")
        # requirement bullet lists live outside the description body on Lever
        for lst in job.get("lists") or []:
            description += " " + _strip_html(lst.get("content") or "")
        records.append(
            {
                "source": "lever",
                "company": site_token,
                "job_id": str(job["id"]),
                "title": job.get("text"),
                "department": categories.get("team") or categories.get("department"),
                "location": categories.get("location"),
                "url": job.get("hostedUrl"),
                "published_at": published_at,
                "description_text": description.strip(),
                "raw_payload": json.dumps(job),
                "ingested_at": ingested_at,
            }
        )
    return records


def fetch_all(boards: dict[str, list[str]]) -> tuple[dict[str, list[dict]], list[str]]:
    """Fetch every configured board.

    ``boards`` maps source name -> list of tokens. Returns (results, failures)
    where results maps "source/company" -> records and failures lists the
    boards that could not be fetched.
    """
    fetchers = {"greenhouse": fetch_greenhouse, "lever": fetch_lever}
    results: dict[str, list[dict]] = {}
    failures: list[str] = []
    for source, tokens in boards.items():
        for token in tokens:
            key = f"{source}/{token}"
            records = fetchers[source](token)
            if records is None:
                failures.append(key)
            else:
                results[key] = records
                logger.info("%s: %d postings", key, len(records))
            time.sleep(REQUEST_DELAY_SECONDS)
    return results, failures
