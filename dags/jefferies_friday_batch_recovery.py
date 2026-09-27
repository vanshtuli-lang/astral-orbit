"""Friday batch failure-and-recovery demonstration.

Main flow:

1. Verify Oracle connectivity and the demo table.
2. Run three ingestion branches in parallel.
3. Transform each branch independently.
4. Join the branches at ``load_database``.
5. Intentionally fail the first attempt of ``load_database``.
6. Inspect the real task state and failure logs with Otto for RCA.
7. Recover by clearing only the failed load task with its downstream tasks.
8. Reconcile the recovered load and run the final downstream handoff task.

The controlled failure happens before Oracle is modified. The failed task logs
its run ID, task ID, try number, and prepared record count so Otto can produce
RCA from actual evidence. When manually cleared, the second attempt succeeds.
The load deletes rows for the current run before inserting, so rerunning it is
safe and needs no manual cleanup.
"""

import json
import logging
from decimal import Decimal

from airflow.sdk import dag, get_current_context, task
from pendulum import datetime

ORACLE_CONN_ID = "oracle_jefferies_settlement"
logger = logging.getLogger(__name__)


def get_oracle_hook():
    """Create the Oracle hook only while a task is running."""
    from airflow.providers.oracle.hooks.oracle import OracleHook

    return OracleHook(oracle_conn_id=ORACLE_CONN_ID)


@dag(
    dag_id="Jefferies_friday_batch_failure_recovery",
    description="Friday batch failure and selective recovery demo",
    start_date=datetime(2026, 9, 26, tz="America/New_York"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "batch-operations", "retries": 2},
    tags=["jefferies", "friday-batch", "recovery", "demo"],
)
def friday_batch_failure_recovery():
    @task
    def preflight_checks() -> None:
        """Confirm the database and intentional-failure setting are ready."""
        hook = get_oracle_hook()
        connection = hook.get_conn()
        cursor = connection.cursor()
        cursor.execute(
            """
            SELECT COUNT(*)
            FROM USER_TABLES
            WHERE TABLE_NAME = 'JEFFERIES_FRIDAY_BATCH_RESULTS'
            """
        )
        if cursor.fetchone()[0] != 1:
            raise ValueError("JEFFERIES_FRIDAY_BATCH_RESULTS does not exist")

        logger.info("Pre-flight checks passed")

    # These three tasks represent independent Friday source streams.
    @task
    def ingest_trades() -> list[dict]:
        logger.info("Ingested two trade records")
        return [
            {"record_id": "TRD-1001", "amount": "1250.00"},
            {"record_id": "TRD-1002", "amount": "750.00"},
        ]

    @task
    def ingest_positions() -> list[dict]:
        logger.info("Ingested two position records")
        return [
            {"record_id": "POS-2001", "amount": "5000.00"},
            {"record_id": "POS-2002", "amount": "3200.00"},
        ]

    @task
    def ingest_reference() -> list[dict]:
        logger.info("Ingested two reference records")
        return [
            {"record_id": "REF-3001", "amount": "0.00"},
            {"record_id": "REF-3002", "amount": "0.00"},
        ]

    @task
    def transform_trades(rows: list[dict]) -> list[dict]:
        logger.info("Transformed %s trade records", len(rows))
        return [{**row, "stream_name": "TRADES"} for row in rows]

    @task
    def transform_positions(rows: list[dict]) -> list[dict]:
        logger.info("Transformed %s position records", len(rows))
        return [{**row, "stream_name": "POSITIONS"} for row in rows]

    @task
    def transform_reference(rows: list[dict]) -> list[dict]:
        logger.info("Transformed %s reference records", len(rows))
        return [{**row, "stream_name": "REFERENCE"} for row in rows]

    @task(retries=0)
    def load_database(
        trades: list[dict],
        positions: list[dict],
        reference: list[dict],
    ) -> int:
        """Join all branches and intentionally fail before touching Oracle."""
        context = get_current_context()
        records = trades + positions + reference
        failure_context = {
            "dag_id": context["dag_run"].dag_id,
            "run_id": context["run_id"],
            "task_id": context["task"].task_id,
            "try_number": context["ti"].try_number,
            "records_ready": len(records),
            "database_modified": False,
        }
        logger.info("Load task context: %s", json.dumps(failure_context, sort_keys=True))

        if context["ti"].try_number == 1:
            logger.error(
                "CONTROLLED_FRIDAY_FAILURE: first load attempt failed. Context=%s",
                json.dumps(failure_context, sort_keys=True),
            )
            raise RuntimeError(
                "Controlled demo failure: clear load_database with its downstream "
                "tasks to continue from this point"
            )

        connection = get_oracle_hook().get_conn()
        cursor = connection.cursor()
        cursor.execute(
            "DELETE FROM JEFFERIES_FRIDAY_BATCH_RESULTS WHERE RUN_ID = :1",
            [context["run_id"]],
        )
        cursor.executemany(
            """
            INSERT INTO JEFFERIES_FRIDAY_BATCH_RESULTS (
                RUN_ID, STREAM_NAME, RECORD_ID, AMOUNT, LOADED_AT
            ) VALUES (:1, :2, :3, :4, SYSTIMESTAMP)
            """,
            [
                (
                    context["run_id"],
                    row["stream_name"],
                    row["record_id"],
                    Decimal(row["amount"]),
                )
                for row in records
            ],
        )
        connection.commit()
        logger.info("Loaded %s records after recovery", len(records))
        return len(records)

    @task
    def reconcile(loaded_count: int) -> dict:
        """Confirm the recovered run loaded every record exactly once."""
        run_id = get_current_context()["run_id"]
        connection = get_oracle_hook().get_conn()
        cursor = connection.cursor()
        cursor.execute(
            """
            SELECT COUNT(*), NVL(SUM(AMOUNT), 0)
            FROM JEFFERIES_FRIDAY_BATCH_RESULTS
            WHERE RUN_ID = :1
            """,
            [run_id],
        )
        actual_count, total_amount = cursor.fetchone()
        if actual_count != loaded_count:
            raise ValueError(
                f"Reconciliation expected {loaded_count} rows but found {actual_count}"
            )

        result = {"record_count": actual_count, "total_amount": str(total_amount)}
        logger.info("Reconciliation passed: %s", result)
        return result

    @task
    def trigger_downstream(reconciliation: dict) -> None:
        """Represent the final handoff to downstream processing."""
        logger.info("Friday batch complete; downstream handoff: %s", reconciliation)

    preflight = preflight_checks()

    trades = ingest_trades()
    positions = ingest_positions()
    reference = ingest_reference()
    preflight >> [trades, positions, reference]

    transformed_trades = transform_trades(trades)
    transformed_positions = transform_positions(positions)
    transformed_reference = transform_reference(reference)

    loaded = load_database(
        transformed_trades,
        transformed_positions,
        transformed_reference,
    )
    reconciled = reconcile(loaded)
    trigger_downstream(reconciled)


friday_batch_failure_recovery()
