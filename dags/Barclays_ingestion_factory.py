"""Barclays self-service Oracle-to-SFTP ingestion demo.

The existing SFTP server represents the S3 landing zone for this demo.

The factory is intentionally small:

1. ``preflight`` confirms Oracle and SFTP are reachable.
2. ``extract_to_staging`` reads one time window from Oracle into a CSV.
3. ``publish_to_landing`` moves that CSV into the final landing folder.

A new tenant is onboarded by adding one entry to
``dags/Barclays_ingestion_tenants.json``. No DAG code needs to be copied.

Production extensions such as detailed data-quality rules, ServiceNow alerts,
schema evolution, and persistent checkpoints can be inserted between extract
and publish without changing the tenant configuration pattern shown here.
"""

from __future__ import annotations

import csv
import json
import logging
import re
from contextlib import closing
from datetime import datetime as python_datetime
from io import StringIO
from pathlib import Path
from typing import Any

import pendulum
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.sdk import dag, get_current_context, task
from pendulum import datetime

# Keep the config beside the DAG so Astro DAG-only bundles deploy both files.
CONFIG_PATH = Path(__file__).with_name("Barclays_ingestion_tenants.json")
IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z][A-Za-z0-9_$#]*$")
LOGGER = logging.getLogger(__name__)


def load_tenant_configs() -> list[dict[str, Any]]:
    """Read the tenant list that controls which DAGs are created."""
    with CONFIG_PATH.open(encoding="utf-8") as config_file:
        return json.load(config_file)["tenants"]


def oracle_identifier(value: str) -> str:
    """Validate table and column names before placing them in Oracle SQL."""
    if not IDENTIFIER_PATTERN.fullmatch(value):
        raise ValueError(f"Invalid Oracle identifier: {value}")
    return value.upper()


def get_oracle_hook(connection_id: str):
    """Build an Oracle hook from the connection ID supplied by the tenant."""
    from airflow.providers.oracle.hooks.oracle import OracleHook

    return OracleHook(oracle_conn_id=connection_id)


def ensure_sftp_directory(sftp, directory: str) -> None:
    """Create the configured SFTP landing folders when they do not exist."""
    current = ""
    for part in directory.strip("/").split("/"):
        current = f"{current}/{part}" if current else part
        try:
            sftp.stat(current)
        except OSError:
            sftp.mkdir(current)


def csv_value(value: Any) -> str:
    """Convert Oracle values into simple values that can be written to CSV."""
    if value is None:
        return ""
    if isinstance(value, (python_datetime, pendulum.DateTime)):
        return value.isoformat()
    return str(value)


def build_ingestion_dag(config: dict[str, Any]):
    """Turn one tenant configuration entry into one Airflow DAG."""
    # These values come directly from the tenant's configuration entry.
    table = oracle_identifier(config["source_table"])
    columns = [oracle_identifier(column) for column in config["columns"]]
    watermark_column = oracle_identifier(config["watermark_column"])
    oracle_conn_id = config["oracle_conn_id"]
    sftp_conn_id = config["sftp_conn_id"]
    landing_path = config["landing_path"]

    @dag(
        dag_id=config["dag_id"],
        description="Simple config-generated Oracle ingestion into SFTP",
        start_date=datetime(2026, 9, 27, tz="America/New_York"),
        schedule=config["schedule"],
        catchup=False,
        is_paused_upon_creation=True,
        max_active_runs=1,
        default_args={"owner": config["owner"], "retries": 2},
        tags=["barclays", "oracle", "sftp", "ingestion-factory", "demo"],
        doc_md=__doc__,
    )
    def generated_ingestion():
        @task
        def preflight() -> None:
            """Check the configured Oracle source and SFTP destination."""
            # Run an empty Oracle query to prove the table and selected columns exist.
            with closing(get_oracle_hook(oracle_conn_id).get_conn()) as connection:
                with closing(connection.cursor()) as cursor:
                    cursor.execute(
                        f"SELECT {', '.join(columns)} FROM {table} WHERE 1 = 0"
                    )

            # Write and remove a tiny file to prove the landing path is writable.
            staging_directory = f"{landing_path}/staging"
            probe_path = f"{staging_directory}/_preflight.txt"
            hook = SFTPHook(ssh_conn_id=sftp_conn_id)
            with hook.get_managed_conn() as sftp:
                ensure_sftp_directory(sftp, staging_directory)
                with sftp.open(probe_path, "w") as probe:
                    probe.write("ok")
                sftp.remove(probe_path)

            LOGGER.info("Pre-flight checks passed for %s", config["tenant"])

        @task
        def extract_to_staging() -> dict[str, Any]:
            """Extract the requested Oracle time window into one staged CSV."""
            context = get_current_context()
            run_config = context["dag_run"].conf or {}

            # Scheduled runs use Airflow's current data interval. For a historical
            # demo, window_start and window_end can be supplied when triggering.
            window_start = run_config.get(
                "window_start",
                context["data_interval_start"].in_timezone("UTC").isoformat(),
            )
            window_end = run_config.get(
                "window_end",
                context["data_interval_end"].in_timezone("UTC").isoformat(),
            )

            # Only rows changed inside this window are read from Oracle.
            sql = (
                f"SELECT {', '.join(columns)} FROM {table} "
                f"WHERE {watermark_column} >= :window_start "
                f"AND {watermark_column} < :window_end "
                f"ORDER BY {watermark_column}"
            )
            with closing(get_oracle_hook(oracle_conn_id).get_conn()) as connection:
                with closing(connection.cursor()) as cursor:
                    cursor.execute(
                        sql,
                        window_start=pendulum.parse(window_start),
                        window_end=pendulum.parse(window_end),
                    )
                    rows = cursor.fetchall()

            # The extracted rows are written to staging first. This is the point
            # where a production implementation could add quality checks.
            output = StringIO()
            writer = csv.writer(output)
            writer.writerow(columns)
            writer.writerows([csv_value(value) for value in row] for row in rows)

            run_id = re.sub(r"[^A-Za-z0-9_.-]", "_", context["run_id"])
            staged_path = f"{landing_path}/staging/{run_id}.csv"
            hook = SFTPHook(ssh_conn_id=sftp_conn_id)
            with hook.get_managed_conn() as sftp:
                ensure_sftp_directory(sftp, f"{landing_path}/staging")
                with sftp.open(staged_path, "w") as staged_file:
                    staged_file.write(output.getvalue())

            LOGGER.info("Extracted %s rows from %s", len(rows), table)
            return {
                "staged_path": staged_path,
                "row_count": len(rows),
                "window_start": window_start,
                "window_end": window_end,
            }

        @task
        def publish_to_landing(extraction: dict[str, Any]) -> None:
            """Move the staged CSV into the tenant's final landing folder."""
            # Partitioning by date keeps the demo landing area easy to browse.
            partition_date = pendulum.parse(extraction["window_end"]).format(
                "YYYY-MM-DD"
            )
            final_directory = f"{landing_path}/landing/date={partition_date}"
            final_path = f"{final_directory}/{Path(extraction['staged_path']).name}"

            hook = SFTPHook(ssh_conn_id=sftp_conn_id)
            with hook.get_managed_conn() as sftp:
                ensure_sftp_directory(sftp, final_directory)
                sftp.rename(extraction["staged_path"], final_path)

            # Airflow task logs provide the simple operational summary for the demo.
            LOGGER.info(
                "Published %s rows | tenant=%s | source=%s | destination=%s",
                extraction["row_count"],
                config["tenant"],
                table,
                final_path,
            )

        preflight_task = preflight()
        extraction = extract_to_staging()
        publication = publish_to_landing(extraction)

        # This explicit chain is the complete workflow shown in the Airflow graph.
        preflight_task >> extraction >> publication

    return generated_ingestion()


# This is the factory: every tenant entry creates one DAG with the same three steps.
for tenant_config in load_tenant_configs():
    globals()[tenant_config["dag_id"]] = build_ingestion_dag(tenant_config)
