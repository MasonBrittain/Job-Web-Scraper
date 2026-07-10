"""Dagster definitions: ingestion asset -> dbt assets, on a daily schedule.

Run locally with ``dagster dev`` from the repo root (module configured in
pyproject.toml), then materialize from the UI or let the schedule fire.
"""
import os
import sys
from pathlib import Path

import dagster as dg
from dagster import AssetExecutionContext
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, DbtProject, dbt_assets

from jobintel.config import DBT_PROJECT_DIR
from jobintel.ingestion.run import ingest

# dagster-dbt shells out to `dbt`; make sure the interpreter's own scripts
# dir is on PATH so this works even when the venv isn't activated
os.environ["PATH"] = str(Path(sys.executable).parent) + os.pathsep + os.environ["PATH"]

dbt_project = DbtProject(project_dir=DBT_PROJECT_DIR)
dbt_project.prepare_if_dev()


@dg.asset(
    group_name="ingestion",
    description="Daily snapshot of all configured Greenhouse/Lever boards, "
    "landed as hive-partitioned parquet (bronze layer).",
)
def raw_job_postings(context: AssetExecutionContext) -> dg.MaterializeResult:
    summary = ingest()

    if summary["boards_ok"] == 0:
        raise RuntimeError(f"Every board failed to fetch: {summary['boards_failed']}")
    if summary["boards_failed"]:
        context.log.warning("Boards failed (skipped): %s", summary["boards_failed"])

    return dg.MaterializeResult(
        metadata={
            "ingest_date": summary["ingest_date"],
            "total_rows": summary["total_rows"],
            "boards_ok": summary["boards_ok"],
            "boards_failed": summary["boards_failed"],
            "rows_per_board": dg.MetadataValue.json(summary["rows_per_board"]),
        }
    )


class JobIntelDbtTranslator(DagsterDbtTranslator):
    """Point the dbt `landing` source at the ingestion asset so the lineage
    graph connects end to end."""

    def get_asset_key(self, dbt_resource_props) -> dg.AssetKey:
        if dbt_resource_props["resource_type"] == "source":
            return dg.AssetKey("raw_job_postings")
        return super().get_asset_key(dbt_resource_props)


@dbt_assets(manifest=dbt_project.manifest_path, dagster_dbt_translator=JobIntelDbtTranslator())
def jobintel_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    # `build` runs seeds, models, and tests in dependency order
    yield from dbt.cli(["build"], context=context).stream()


daily_refresh = dg.define_asset_job("daily_refresh", selection=dg.AssetSelection.all())

daily_schedule = dg.ScheduleDefinition(
    job=daily_refresh,
    cron_schedule="0 6 * * *",  # 06:00 UTC, before US business hours
    default_status=dg.DefaultScheduleStatus.RUNNING,
)

defs = dg.Definitions(
    assets=[raw_job_postings, jobintel_dbt_assets],
    schedules=[daily_schedule],
    resources={"dbt": DbtCliResource(project_dir=dbt_project)},
)
