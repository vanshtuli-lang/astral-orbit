"""Barclays card settlement reconciliation, driven by sensors.

Each morning the card network sends Barclays a settlement file over SFTP. Ops
can only reconcile it once our own transactions for the day are in Oracle. The
two arrive at different times, and neither one keeps a fixed schedule.

Instead of a cron job and a hope, this DAG waits for both:

1. ``wait_for_settlement_file`` watches SFTP for the card network's file.
2. ``wait_for_transactions_load`` polls Oracle until today's transactions
   are loaded into ``BARCLAYS_CUSTOMER_TRANSACTIONS``.
3. As soon as both are ready, the file is reconciled against Oracle.

The two sensors wait in different ways. The SFTP sensor is deferrable, so the
triggerer does the waiting and no worker slot is used. The SQL sensor runs in
reschedule mode, so it gives its worker slot back between checks. Both time
out after two hours, so a missing file or load fails the run instead of
leaving it hanging.

The file is expected at ``Barclays_ingestion/incoming/card_settlement.csv``
with at least a ``TRANSACTION_ID`` column. This DAG only reads data.
"""

from __future__ import annotations

import csv
import logging
from contextlib import closing
from io import StringIO

from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.sftp.sensors.sftp import SFTPSensor
from airflow.sdk import dag, task
from pendulum import datetime

# Same connections the Barclays ingestion factory uses.
ORACLE_CONN_ID = "oracle_jefferies_settlement"
SFTP_CONN_ID = "sftp_claims"

SETTLEMENT_FILE = "Barclays_ingestion/incoming/card_settlement.csv"
TRANSACTIONS_TABLE = "BARCLAYS_CUSTOMER_TRANSACTIONS"
LOGGER = logging.getLogger(__name__)


def get_oracle_hook():
    """Create the Oracle hook inside a task, not while the DAG is parsing."""
    from airflow.providers.oracle.hooks.oracle import OracleHook

    return OracleHook(oracle_conn_id=ORACLE_CONN_ID)


@dag(
    dag_id="Barclays_card_settlement_sensors",
    description="Wait for the card settlement file and today's Oracle load, then reconcile",
    start_date=datetime(2026, 10, 1, tz="America/New_York"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "edp-ingestion", "retries": 1},
    tags=["barclays", "sensors", "oracle", "sftp", "demo"],
    doc_md=__doc__,
)
def barclays_card_settlement_sensors():
    # Waits for the card network's file. With deferrable=True the check runs
    # on the triggerer, and the task shows as "deferred" until the file lands.
    wait_for_settlement_file = SFTPSensor(
        task_id="wait_for_settlement_file",
        sftp_conn_id=SFTP_CONN_ID,
        path=SETTLEMENT_FILE,
        deferrable=True,
        poke_interval=30,
        timeout=2 * 60 * 60,
    )

    # Waits until today's transactions are in Oracle. The sensor succeeds once
    # the query returns a count above zero. Reschedule mode frees the worker
    # between checks, and the task shows as "up_for_reschedule" while waiting.
    wait_for_transactions_load = SqlSensor(
        task_id="wait_for_transactions_load",
        conn_id=ORACLE_CONN_ID,
        sql=f"""
            SELECT COUNT(*)
            FROM {TRANSACTIONS_TABLE}
            WHERE LAST_UPDATED_TS >= TRUNC(SYSDATE)
        """,
        success=lambda row_count: row_count > 0,
        mode="reschedule",
        poke_interval=60,
        timeout=2 * 60 * 60,
    )

    @task
    def reconcile_transactions() -> dict:
        """Compare transaction IDs in the settlement file with Oracle."""
        hook = SFTPHook(ssh_conn_id=SFTP_CONN_ID)
        with hook.get_managed_conn() as sftp:
            with sftp.open(SETTLEMENT_FILE, "r") as remote_file:
                contents = remote_file.read()
        if isinstance(contents, bytes):
            contents = contents.decode("utf-8")
        file_ids = {row["TRANSACTION_ID"] for row in csv.DictReader(StringIO(contents))}

        with closing(get_oracle_hook().get_conn()) as connection:
            with closing(connection.cursor()) as cursor:
                cursor.execute(f"SELECT TRANSACTION_ID FROM {TRANSACTIONS_TABLE}")
                oracle_ids = {str(row[0]) for row in cursor.fetchall()}

        unmatched = sorted(file_ids - oracle_ids)
        if unmatched:
            LOGGER.warning("Not found in Oracle: %s", unmatched)

        return {
            "file_rows": len(file_ids),
            "matched": len(file_ids & oracle_ids),
            "unmatched": len(unmatched),
        }

    @task
    def publish_summary(summary: dict) -> None:
        """Log the reconciliation result for the ops team."""
        LOGGER.info(
            "Card settlement reconciled | file rows=%s | matched=%s | unmatched=%s",
            summary["file_rows"],
            summary["matched"],
            summary["unmatched"],
        )

    summary = reconcile_transactions()
    [wait_for_settlement_file, wait_for_transactions_load] >> summary
    publish_summary(summary)


barclays_card_settlement_sensors()
