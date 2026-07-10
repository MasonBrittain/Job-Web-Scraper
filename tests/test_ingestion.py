"""Unit tests for the ingestion layer: API normalization and parquet landing.

These run fully offline — HTTP fetches are monkeypatched with canned payloads
shaped like real Greenhouse/Lever responses.
"""
from datetime import date

import pandas as pd
import pytest

from jobintel.ingestion import landing, sources
from jobintel.ingestion.sources import _strip_html, fetch_greenhouse, fetch_lever

GREENHOUSE_PAYLOAD = {
    "jobs": [
        {
            "id": 123,
            "title": "Data Engineer",
            "content": "&lt;p&gt;We use &lt;b&gt;Python&lt;/b&gt; &amp;amp; SQL&lt;/p&gt;",
            "location": {"name": "New York"},
            "absolute_url": "https://example.com/jobs/123",
            "updated_at": "2026-07-01T09:00:00-04:00",
            "departments": [{"name": "Data Platform"}],
        }
    ]
}

LEVER_PAYLOAD = [
    {
        "id": "abc-def",
        "text": "Analytics Engineer",
        "categories": {"team": "Data", "location": "Remote"},
        "hostedUrl": "https://example.com/lever/abc-def",
        "createdAt": 1780000000000,
        "descriptionPlain": "Own our dbt models.",
        "lists": [{"content": "<li>Advanced SQL</li>"}],
    }
]


class TestStripHtml:
    def test_removes_tags_and_entities(self):
        assert _strip_html("<p>Python &amp; <b>SQL</b></p>") == "Python & SQL"

    def test_collapses_whitespace(self):
        assert _strip_html("a\n\n  b\t c") == "a b c"

    def test_handles_empty(self):
        assert _strip_html("") == ""


class TestGreenhouse:
    def test_normalizes_posting(self, monkeypatch):
        monkeypatch.setattr(sources, "_get_json", lambda url: GREENHOUSE_PAYLOAD)
        records = fetch_greenhouse("acme")

        assert len(records) == 1
        rec = records[0]
        assert rec["source"] == "greenhouse"
        assert rec["company"] == "acme"
        assert rec["job_id"] == "123"
        assert rec["department"] == "Data Platform"
        # content is double-escaped HTML upstream; both layers must be undone
        assert rec["description_text"] == "We use Python & SQL"

    def test_unreachable_board_returns_none(self, monkeypatch):
        monkeypatch.setattr(sources, "_get_json", lambda url: None)
        assert fetch_greenhouse("gone") is None


class TestLever:
    def test_normalizes_posting(self, monkeypatch):
        monkeypatch.setattr(sources, "_get_json", lambda url: LEVER_PAYLOAD)
        records = fetch_lever("acme")

        assert len(records) == 1
        rec = records[0]
        assert rec["source"] == "lever"
        assert rec["job_id"] == "abc-def"
        assert rec["department"] == "Data"
        assert rec["published_at"].startswith("2026-")
        # requirement bullet lists live outside the main description
        assert "Advanced SQL" in rec["description_text"]

    def test_unreachable_board_returns_none(self, monkeypatch):
        monkeypatch.setattr(sources, "_get_json", lambda url: None)
        assert fetch_lever("gone") is None


class TestLanding:
    @pytest.fixture
    def record(self):
        return {
            "source": "greenhouse",
            "company": "acme",
            "job_id": "123",
            "title": "Data Engineer",
            "department": "Data",
            "location": "NYC",
            "url": "https://example.com/jobs/123",
            "published_at": "2026-07-01T09:00:00-04:00",
            "description_text": "Python & SQL",
            "raw_payload": "{}",
            "ingested_at": "2026-07-10T06:00:00+00:00",
        }

    def test_writes_hive_partitioned_parquet(self, tmp_path, monkeypatch, record):
        monkeypatch.setattr(landing, "RAW_DIR", tmp_path)
        path = landing.write_partition([record], "greenhouse", "acme", date(2026, 7, 10))

        assert "ingest_date=2026-07-10" in path
        frame = pd.read_parquet(path)
        assert len(frame) == 1
        assert frame.loc[0, "job_id"] == "123"
        # partition keys live in the path, not the file
        assert "source" not in frame.columns
        assert "company" not in frame.columns

    def test_rerun_overwrites_instead_of_appending(self, tmp_path, monkeypatch, record):
        monkeypatch.setattr(landing, "RAW_DIR", tmp_path)
        landing.write_partition([record], "greenhouse", "acme", date(2026, 7, 10))
        landing.write_partition([record], "greenhouse", "acme", date(2026, 7, 10))

        files = list(tmp_path.rglob("*.parquet"))
        assert len(files) == 1
        assert len(pd.read_parquet(files[0])) == 1
