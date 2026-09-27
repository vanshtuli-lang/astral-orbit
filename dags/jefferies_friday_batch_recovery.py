"""Friday batch failure-and-recovery demonstration.

Main flow:

1. Verify Oracle connectivity and the demo table.
2. Run three ingestion branches in parallel.
3. Transform each branch independently.
4. Join the branches at ``load_database``.
5. Intentionally fail the first run when ``friday_batch_force_failure`` is true.
6. Run an RCA task even though the load failed.
7. Recover by setting the Variable to false and clearing only the failed load
   task with its downstream tasks.
8. Reconcile the recovered load and emit an Asset event that triggers the
   downstream confirmation DAG.

The controlled failure happens before Oracle is modified. The load also deletes
rows for the current run before inserting, so rerunning it is safe and requires
no manual database cleanup.
"""

import json
import logging
from decimal import Decimal

from airflow.sdk import Asset, Variable, dag, get_current_context, task
from pendulum import datetime

ORACLE_CONN_ID = "oracle_jefferies_settlement"
FAILURE_VARIABLE = "friday_batch_force_failure"
FRIDAY_BATCH_COMPLETE = Asset("jefferies://friday-batch/complete")
logger = logging.getLogger(__name__)


def get_oracle_hook():
    """Create the Oracle hook only while a task is running."""
    from airflow.providers.oracle.hooks.oracle import OracleHook

    return OracleHook(oracle_conn_id=ORACLE_CONN_ID)


def failure_enabled() -> bool:
    """Default to failure so the first demonstration is deterministic."""
    return str(Variable.get(FAILURE_VARIABLE, default="true")).lower() == "true"


@dag(
    dag_id="Jefferies_friday_batch_failure_recovery",
    description="Friday batch failure, RCA, and selective recovery demo",
    start_date=datetime(2026, 9, 26, tz="America/New_York"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "batch-operations", "retries": 2},
    tags=["jefferies", "friday-batch", "recovery", "rca", "demo"],
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

        logger.info(
            "Pre-flight complete. %s=%s",
            FAILURE_VARIABLE,
            failure_enabled(),
        )

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
            "failure_variable": FAILURE_VARIABLE,
            "failure_enabled": failure_enabled(),
            "database_modified": False,
        }
        logger.info("Load task context: %s", json.dumps(failure_context, sort_keys=True))

        if failure_context["failure_enabled"]:
            logger.error(
                "CONTROLLED_FRIDAY_FAILURE: load blocked by %s. Context=%s",
                FAILURE_VARIABLE,
                json.dumps(failure_context, sort_keys=True),
            )
            raise RuntimeError(
                f"Controlled demo failure: set {FAILURE_VARIABLE}=false, then clear "
                "load_database with its downstream tasks"
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

    @task(outlets=[FRIDAY_BATCH_COMPLETE])
    def trigger_downstream(reconciliation: dict) -> None:
        """Emit a real Asset event for the downstream confirmation DAG."""
        logger.info("Friday batch complete; publishing downstream event: %s", reconciliation)

    @task(trigger_rule="all_done")
    def rca_summary() -> dict:
        """Provide a concise operator handoff after failure or recovery."""
        if failure_enabled():
            summary = {
                "failed_task": "load_database",
                "error_source": f"Controlled setting {FAILURE_VARIABLE}=true",
                "completed_work": [
                    "preflight_checks",
                    "all three ingestion tasks",
                    "all three transformation tasks",
                ],
                "affected_downstream": ["reconcile", "trigger_downstream"],
                "database_cleanup_required": False,
                "recommended_next_action": (
                    f"Set {FAILURE_VARIABLE}=false, then clear load_database "
                    "with Downstream selected"
                ),
            }
            logger.error("RCA SUMMARY\n%s", json.dumps(summary, indent=2))
        else:
            summary = {
                "recovery_status": "Controlled issue corrected",
                "rerun_scope": ["load_database", "reconcile", "trigger_downstream"],
                "database_cleanup_required": False,
                "recommended_next_action": "Verify reconciliation and downstream run",
            }
            logger.info("RECOVERY SUMMARY\n%s", json.dumps(summary, indent=2))
        return summary

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

    rca = rca_summary()
    loaded >> rca


@dag(
    dag_id="Jefferies_friday_batch_downstream",
    description="Downstream confirmation triggered by the recovered Friday batch",
    start_date=datetime(2026, 9, 26, tz="America/New_York"),
    schedule=[FRIDAY_BATCH_COMPLETE],
    catchup=False,
    default_args={"owner": "batch-operations", "retries": 2},
    tags=["jefferies", "friday-batch", "downstream", "demo"],
)
def friday_batch_downstream():
    @task
    def confirm_downstream_start() -> None:
        logger.info("Downstream Friday processing started from the completion Asset event")

    confirm_downstream_start()


friday_batch_failure_recovery()
friday_batch_downstream()
