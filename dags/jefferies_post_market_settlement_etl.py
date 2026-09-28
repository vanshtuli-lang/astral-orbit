"""Jefferies post-market settlement demo.

Overall flow:

1. Wait for the settlement CSV in the demo's own Azure SFTP folder.
2. Validate that the CSV has rows and the expected columns.
3. Copy the rows into a simple Oracle staging table.
4. Transform each row by calculating ``notional = quantity * price``.
5. Pause for an operator decision in Airflow's Required Actions UI.
6. On Approve, load the final settlement table; on Reject, ice the stream.
7. Reconcile the staging and final tables.
8. Pass the visible ``stream_complete`` gate and publish a completion message.
9. Copy the processed file into the demo's own SFTP archive folder while
   leaving the incoming file available for the next demo run.

The existing claims files and folders are not touched. This DAG only uses paths
under ``jefferies_settlement/`` in the ``inbound`` container. Airflow uses the
secret ``sftp_claims`` connection, so no SFTP credential is stored in this
repository. Airflow's Grid view provides the task audit trail.

Prerequisite: create ``JEFFERIES_SETTLEMENT_STAGE`` and
``JEFFERIES_SETTLEMENTS`` once in the existing staging schema used by the
``oracle_jefferies_settlement`` connection.
"""

import csv
from datetime import timedelta
from decimal import Decimal
from io import StringIO

from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.sftp.sensors.sftp import SFTPSensor
from airflow.providers.standard.operators.hitl import ApprovalOperator
from airflow.sdk import dag, get_current_context, task
from pendulum import datetime

SFTP_CONN_ID = "sftp_claims"
ORACLE_CONN_ID = "oracle_jefferies_settlement"
SETTLEMENT_FILE = "jefferies_settlement/incoming/settlement_data.csv"
ARCHIVE_DIR = "jefferies_settlement/archive"


def get_oracle_hook():
    """Create the Oracle hook inside a task, never while the DAG is parsing."""
    from airflow.providers.oracle.hooks.oracle import OracleHook

    return OracleHook(oracle_conn_id=ORACLE_CONN_ID)


def read_sftp_text(remote_path: str) -> str:
    """Read one small demo file through the Key Vault-backed SFTP connection."""
    hook = SFTPHook(ssh_conn_id=SFTP_CONN_ID)
    with hook.get_managed_conn() as sftp:
        with sftp.open(remote_path, "r") as remote_file:
            contents = remote_file.read()
    return contents.decode("utf-8") if isinstance(contents, bytes) else contents


@dag(
    dag_id="Jefferies_post_market_settlement_etl",
    description="Simple settlement ETL demo with a manual release/ice step",
    start_date=datetime(2026, 9, 26, tz="America/New_York"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "settlement-operations", "retries": 2},
    tags=["jefferies", "settlement", "sftp", "azure", "demo", "hitl"],
)
def jefferies_post_market_settlement_etl():
    # The sensor checks only our dedicated folder. Deferrable mode means it
    # does not occupy a worker while waiting for the file.
    wait_for_file = SFTPSensor(
        task_id="wait_for_file",
        sftp_conn_id=SFTP_CONN_ID,
        path=SETTLEMENT_FILE,
        poke_interval=30,
        timeout=1800,
        deferrable=True,
    )

    @task
    def validate_file(remote_path: str) -> dict:
        """Confirm that the CSV is non-empty and has the expected columns."""
        rows = list(csv.DictReader(StringIO(read_sftp_text(remote_path))))
        expected_columns = {"trade_id", "account_id", "quantity", "price"}

        if not rows:
            raise ValueError("Settlement CSV is empty")
        if set(rows[0]) != expected_columns:
            raise ValueError(f"Settlement CSV must contain {sorted(expected_columns)}")

        print(f"Validated {len(rows)} settlement rows")
        return {"record_count": len(rows), "remote_path": remote_path}

    @task
    def stage_file(validation: dict) -> int:
        """Copy the small CSV into the Oracle staging table."""
        rows = list(
            csv.DictReader(StringIO(read_sftp_text(validation["remote_path"])))
        )
        run_id = get_current_context()["run_id"]

        connection = get_oracle_hook().get_conn()
        cursor = connection.cursor()
        cursor.execute("DELETE FROM JEFFERIES_SETTLEMENT_STAGE")
        cursor.executemany(
            """
            INSERT INTO JEFFERIES_SETTLEMENT_STAGE (
                RUN_ID, TRADE_ID, ACCOUNT_ID, QUANTITY, PRICE, NOTIONAL
            ) VALUES (:1, :2, :3, :4, :5, NULL)
            """,
            [
                (
                    run_id,
                    row["trade_id"],
                    row["account_id"],
                    Decimal(row["quantity"]),
                    Decimal(row["price"]),
                )
                for row in rows
            ],
        )
        connection.commit()
        return len(rows)

    @task
    def transform_rows(staged_count: int) -> int:
        """Apply one visible transformation: quantity times price."""
        connection = get_oracle_hook().get_conn()
        cursor = connection.cursor()
        cursor.execute(
            "UPDATE JEFFERIES_SETTLEMENT_STAGE SET NOTIONAL = QUANTITY * PRICE"
        )
        connection.commit()
        print(f"Calculated notional for {staged_count} staged rows")
        return staged_count

    # This task appears in Airflow's Required Actions UI. Do nothing to hold
    # the stream, choose Release to continue, or choose Ice to stop the load.
    release_or_ice = ApprovalOperator(
        task_id="hold_or_ice_before_load",
        subject="Release or ice the Jefferies settlement load",
        body=(
            "The input passed validation and transformation.\n\n"
            f"- Input: `{SETTLEMENT_FILE}`\n\n"
            "Choose **Approve** to release the load or **Reject** to ice the stream."
        ),
        defaults="Reject",
        response_timeout=timedelta(hours=12),
    )

    @task
    def load_settlements() -> int:
        """Replace the demo settlement table with the transformed rows."""
        connection = get_oracle_hook().get_conn()
        cursor = connection.cursor()
        cursor.execute("DELETE FROM JEFFERIES_SETTLEMENTS")
        cursor.execute(
            """
            INSERT INTO JEFFERIES_SETTLEMENTS (
                TRADE_ID, ACCOUNT_ID, QUANTITY, PRICE, NOTIONAL, LOADED_AT
            )
            SELECT TRADE_ID, ACCOUNT_ID, QUANTITY, PRICE, NOTIONAL, SYSTIMESTAMP
            FROM JEFFERIES_SETTLEMENT_STAGE
            """
        )
        loaded_count = cursor.rowcount
        connection.commit()
        print(f"Loaded {loaded_count} settlement rows")
        return loaded_count

    @task
    def reconcile(loaded_count: int) -> dict:
        """Confirm that stage and final counts and totals agree."""
        connection = get_oracle_hook().get_conn()
        cursor = connection.cursor()
        cursor.execute(
            "SELECT COUNT(*), NVL(SUM(NOTIONAL), 0) FROM JEFFERIES_SETTLEMENT_STAGE"
        )
        stage_count, stage_total = cursor.fetchone()
        cursor.execute(
            "SELECT COUNT(*), NVL(SUM(NOTIONAL), 0) FROM JEFFERIES_SETTLEMENTS"
        )
        final_count, final_total = cursor.fetchone()

        if (stage_count, stage_total) != (final_count, final_total):
            raise ValueError("Stage and final settlement totals do not match")
        if final_count != loaded_count:
            raise ValueError("Loaded row count does not match reconciliation")

        return {"record_count": final_count, "total_notional": str(final_total)}

    @task
    def stream_complete(reconciliation: dict) -> dict:
        """A visible downstream gate: all hard dependencies succeeded."""
        print(f"Settlement stream is complete: {reconciliation}")
        return reconciliation

    @task
    def publish_completion(reconciliation: dict) -> None:
        """Publish the final result in the task log for operators to see."""
        print(
            "Settlement load published: "
            f"{reconciliation['record_count']} rows, "
            f"total notional {reconciliation['total_notional']}"
        )

    validation = validate_file(SETTLEMENT_FILE)
    staged = stage_file(validation)
    transformed = transform_rows(staged)
    loaded = load_settlements()
    reconciled = reconcile(loaded)
    complete = stream_complete(reconciled)
    published = publish_completion(complete)

    @task
    def archive_file() -> None:
        """Copy the processed file to the archive and retain the incoming copy."""
        logical_date = get_current_context()["logical_date"]
        timestamp = logical_date.strftime("%Y%m%dT%H%M%S")
        archive_path = f"{ARCHIVE_DIR}/settlement_data_{timestamp}.csv"

        hook = SFTPHook(ssh_conn_id=SFTP_CONN_ID)
        hook.create_directory(ARCHIVE_DIR)
        with hook.get_managed_conn() as sftp:
            with sftp.open(SETTLEMENT_FILE, "rb") as source:
                with sftp.open(archive_path, "wb") as destination:
                    while chunk := source.read(1024 * 1024):
                        destination.write(chunk)
        print(f"Copied {SETTLEMENT_FILE} to {archive_path}")

    archived = archive_file()

    # These explicit dependencies make the demo read like the batch plan.
    wait_for_file >> validation
    transformed >> release_or_ice >> loaded
    reconciled >> complete >> published >> archived


jefferies_post_market_settlement_etl()
