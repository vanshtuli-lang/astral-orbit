"""Simple Barclays Oracle-to-SFTP ingestion factory.

The existing SFTP server represents the final S3-style landing zone for this
demo. Each tenant adds one small entry to
``dags/Barclays_ingestion_tenants.json`` and the factory creates a two-task DAG:

1. Read the complete Oracle table and write one staged CSV.
2. Move the staged CSV into the tenant's final landing folder.

The first task also provides the demo's basic checks. If Oracle is unavailable,
the table does not exist, or SFTP is not writable, that task fails with the
provider's connection or permission error.
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

from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.sdk import dag, get_current_context, task
from pendulum import datetime

# Keep the config beside the DAG so Astro DAG-only bundles deploy both files.
CONFIG_PATH = Path(__file__).with_name("Barclays_ingestion_tenants.json")
IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z][A-Za-z0-9_$#]*$")
LOGGER = logging.getLogger(__name__)


def load_tenant_configs() -> list[dict[str, Any]]:
    """Read the tenant entries that tell the factory which DAGs to create."""
    with CONFIG_PATH.open(encoding="utf-8") as config_file:
        return json.load(config_file)["tenants"]


def oracle_identifier(value: str) -> str:
    """Validate a configured Oracle table or column name before using it in SQL."""
    if not IDENTIFIER_PATTERN.fullmatch(value):
        raise ValueError(f"Invalid Oracle identifier: {value}")
    return value.upper()


def get_oracle_hook(connection_id: str):
    """Create an Oracle hook using the connection ID from tenant configuration."""
    from airflow.providers.oracle.hooks.oracle import OracleHook

    return OracleHook(oracle_conn_id=connection_id)


def ensure_sftp_directory(sftp, directory: str) -> None:
    """Create the configured SFTP folders when they do not already exist."""
    current = ""
    for part in directory.strip("/").split("/"):
        current = f"{current}/{part}" if current else part
        try:
            sftp.stat(current)
        except OSError:
            sftp.mkdir(current)


def csv_value(value: Any) -> str:
    """Convert Oracle values into simple text values for the output CSV."""
    if value is None:
        return ""
    if isinstance(value, python_datetime):
        return value.isoformat()
    return str(value)


def build_ingestion_dag(config: dict[str, Any]):
    """Turn one tenant configuration entry into one two-task Airflow DAG."""
    # Pull the source, destination, and connection details from this tenant's config.
    table = oracle_identifier(config["source_table"])
    columns = [oracle_identifier(column) for column in config["columns"]]
    oracle_conn_id = config["oracle_conn_id"]
    sftp_conn_id = config["sftp_conn_id"]
    landing_path = config["landing_path"]

    @dag(
        dag_id=config["dag_id"],
        description="Config-generated full-table Oracle export into SFTP",
        start_date=datetime(2026, 9, 28, tz="America/New_York"),
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
        def extract_table_to_csv() -> dict[str, Any]:
            """Read the complete Oracle table and write it to SFTP staging."""
            context = get_current_context()

            # Running this query confirms that Oracle is reachable and that the
            # configured table and columns exist. Any problem fails this task.
            sql = f"SELECT {', '.join(columns)} FROM {table}"
            with closing(get_oracle_hook(oracle_conn_id).get_conn()) as connection:
                with closing(connection.cursor()) as cursor:
                    cursor.execute(sql)
                    rows = cursor.fetchall()

            # Convert the Oracle result into one easy-to-inspect CSV file.
            output = StringIO()
            writer = csv.writer(output)
            writer.writerow(columns)
            writer.writerows([csv_value(value) for value in row] for row in rows)

            # Writing the staged file is the basic SFTP connectivity and permission
            # check. If SFTP is unavailable or read-only, this task fails here.
            run_id = re.sub(r"[^A-Za-z0-9_.-]", "_", context["run_id"])
            staging_directory = f"{landing_path}/staging"
            staged_path = f"{staging_directory}/{run_id}.csv"
            hook = SFTPHook(ssh_conn_id=sftp_conn_id)
            with hook.get_managed_conn() as sftp:
                ensure_sftp_directory(sftp, staging_directory)
                with sftp.open(staged_path, "w") as staged_file:
                    staged_file.write(output.getvalue())

            LOGGER.info(
                "Extracted the complete %s table: %s rows written to %s",
                table,
                len(rows),
                staged_path,
            )
            return {"staged_path": staged_path, "row_count": len(rows)}

        @task
        def publish_csv(extraction: dict[str, Any]) -> None:
            """Move the staged CSV into the tenant's final landing folder."""
            context = get_current_context()

            # Use the Airflow run date to keep each demo load easy to locate.
            load_date = context["logical_date"].in_timezone("UTC").format("YYYY-MM-DD")
            final_directory = f"{landing_path}/landing/load_date={load_date}"
            file_name = Path(extraction["staged_path"]).name
            final_path = f"{final_directory}/{file_name}"

            # Rename the staged file into the final area. This represents publishing
            # the Oracle snapshot to the S3-style landing zone used in the demo.
            hook = SFTPHook(ssh_conn_id=sftp_conn_id)
            with hook.get_managed_conn() as sftp:
                ensure_sftp_directory(sftp, final_directory)
                sftp.rename(extraction["staged_path"], final_path)

            LOGGER.info(
                "Published %s rows | tenant=%s | source=%s | destination=%s",
                extraction["row_count"],
                config["tenant"],
                table,
                final_path,
            )

        # The complete workflow is intentionally visible as two simple steps.
        publish_csv(extract_table_to_csv())

    return generated_ingestion()


# This is the factory: every tenant entry creates a separate two-task DAG.
for tenant_config in load_tenant_configs():
    globals()[tenant_config["dag_id"]] = build_ingestion_dag(tenant_config)
