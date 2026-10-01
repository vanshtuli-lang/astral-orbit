"""Task callables for the dag-factory flavor of the Barclays Oracle-to-SFTP ingestion.

``Barclays_ingestion_dagfactory.yml`` wires these into DAGs. Each tenant's
connection IDs, table, columns, and landing path arrive as task arguments
from the YAML instead of from a Python closure.
"""

from __future__ import annotations

import csv
import logging
import re
from contextlib import closing
from datetime import datetime as python_datetime
from io import StringIO
from pathlib import Path
from typing import Any

from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.sdk import get_current_context

IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z][A-Za-z0-9_$#]*$")
LOGGER = logging.getLogger(__name__)


def oracle_identifier(value: str) -> str:
    """Validate a configured Oracle table or column name before using it in SQL."""
    if not IDENTIFIER_PATTERN.fullmatch(value):
        raise ValueError(f"Invalid Oracle identifier: {value}")
    return value.upper()


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


def extract_table_to_csv(
    oracle_conn_id: str,
    source_table: str,
    columns: list[str],
    sftp_conn_id: str,
    landing_path: str,
) -> dict[str, Any]:
    """Read the complete Oracle table and write it to SFTP staging."""
    from airflow.providers.oracle.hooks.oracle import OracleHook

    context = get_current_context()
    table = oracle_identifier(source_table)
    column_names = [oracle_identifier(column) for column in columns]

    # Running this query confirms that Oracle is reachable and that the
    # configured table and columns exist. Any problem fails this task.
    sql = f"SELECT {', '.join(column_names)} FROM {table}"
    with closing(OracleHook(oracle_conn_id=oracle_conn_id).get_conn()) as connection:
        with closing(connection.cursor()) as cursor:
            cursor.execute(sql)
            rows = cursor.fetchall()

    output = StringIO()
    writer = csv.writer(output)
    writer.writerow(column_names)
    writer.writerows([csv_value(value) for value in row] for row in rows)

    # Writing the staged file is the basic SFTP connectivity and permission
    # check. If SFTP is unavailable or read-only, this task fails here.
    run_id = re.sub(r"[^A-Za-z0-9_.-]", "_", context["run_id"])
    staging_directory = f"{landing_path}/staging"
    staged_path = f"{staging_directory}/{run_id}.csv"
    with SFTPHook(ssh_conn_id=sftp_conn_id).get_managed_conn() as sftp:
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


def publish_csv(
    extraction: dict[str, Any],
    tenant: str,
    source_table: str,
    sftp_conn_id: str,
    landing_path: str,
) -> None:
    """Move the staged CSV into the tenant's final landing folder."""
    context = get_current_context()

    load_date = context["logical_date"].in_timezone("UTC").format("YYYY-MM-DD")
    final_directory = f"{landing_path}/landing/load_date={load_date}"
    final_path = f"{final_directory}/{Path(extraction['staged_path']).name}"

    with SFTPHook(ssh_conn_id=sftp_conn_id).get_managed_conn() as sftp:
        ensure_sftp_directory(sftp, final_directory)
        sftp.rename(extraction["staged_path"], final_path)

    LOGGER.info(
        "Published %s rows | tenant=%s | source=%s | destination=%s",
        extraction["row_count"],
        tenant,
        source_table.upper(),
        final_path,
    )
