"""Jefferies post-market settlement demo.

Overall flow:

1. Wait for one settlement CSV to arrive in S3.
2. Validate that the CSV has rows and the expected columns.
3. Copy the rows into a simple Oracle staging table.
4. Transform each row by calculating ``notional = quantity * price``.
5. Pause for an operator decision in Airflow's Required Actions UI.
6. On Approve, load the final settlement table; on Reject, ice the stream.
7. Reconcile the staging and final tables.
8. Pass the visible ``stream_complete`` gate and publish a completion message.

The DAG runs only when triggered. An operator can override ``input_key`` in the
trigger form to process a corrected S3 file immediately, without changing code.
Airflow's Grid view provides the per-task state and execution audit trail.
"""

import csv
from datetime import timedelta
from decimal import Decimal
from io import StringIO

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.standard.operators.hitl import ApprovalOperator
from airflow.sdk import Param, dag, get_current_context, task
from pendulum import datetime

AWS_CONN_ID = "aws_jefferies_settlement"
ORACLE_CONN_ID = "oracle_jefferies_settlement"


def get_oracle_hook():
    """Create the Oracle hook inside a task, never while the DAG is parsing."""
    from airflow.providers.oracle.hooks.oracle import OracleHook

    return OracleHook(oracle_conn_id=ORACLE_CONN_ID)


def read_s3_text(bucket: str, key: str) -> str:
    """Read one small demo file from S3."""
    s3_object = S3Hook(aws_conn_id=AWS_CONN_ID).get_key(key, bucket)
    return s3_object.get()["Body"].read().decode("utf-8")


@dag(
    dag_id="Jefferies_post_market_settlement_etl",
    description="Simple settlement ETL demo with a manual release/ice step",
    start_date=datetime(2026, 9, 26, tz="America/New_York"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "settlement-operations", "retries": 2},
    params={
        # Operators can change these values when triggering a run. That change
        # takes effect immediately without editing or redeploying the DAG.
        "input_key": Param(
            "incoming/revised/jefferies_settlement_20260926.csv",
            type="string",
            description="S3 key for the settlement CSV",
        ),
    },
    tags=["jefferies", "settlement", "demo", "hitl"],
)
def jefferies_post_market_settlement_etl():
    @task
    def create_demo_tables() -> None:
        """Create two intentionally simple tables in JEFFERIES_DEMO."""
        table_sql = {
            "SETTLEMENT_STAGE_DEMO": """
                CREATE TABLE SETTLEMENT_STAGE_DEMO (
                    RUN_ID VARCHAR2(250),
                    TRADE_ID VARCHAR2(50),
                    ACCOUNT_ID VARCHAR2(50),
                    QUANTITY NUMBER,
                    PRICE NUMBER,
                    NOTIONAL NUMBER
                )
            """,
            "SETTLEMENTS_DEMO": """
                CREATE TABLE SETTLEMENTS_DEMO (
                    TRADE_ID VARCHAR2(50),
                    ACCOUNT_ID VARCHAR2(50),
                    QUANTITY NUMBER,
                    PRICE NUMBER,
                    NOTIONAL NUMBER,
                    LOADED_AT TIMESTAMP
                )
            """,
        }

        connection = get_oracle_hook().get_conn()
        cursor = connection.cursor()
        cursor.execute("SELECT TABLE_NAME FROM USER_TABLES")
        existing_tables = {row[0] for row in cursor.fetchall()}

        for table_name, sql in table_sql.items():
            if table_name not in existing_tables:
                cursor.execute(sql)
        connection.commit()

    # The sensor waits for the one self-contained CSV file. Deferrable mode
    # means it does not hold a worker while it waits.
    wait_for_files = S3KeySensor(
        task_id="wait_for_settlement_files",
        aws_conn_id=AWS_CONN_ID,
        bucket_name="{{ var.value.jefferies_settlement_bucket }}",
        bucket_key="{{ params.input_key }}",
        deferrable=True,
        poke_interval=30,
        timeout=6 * 60 * 60,
    )

    @task
    def validate_file(bucket: str, input_key: str) -> dict:
        """Confirm that the CSV is non-empty and has the expected columns."""
        rows = list(csv.DictReader(StringIO(read_s3_text(bucket, input_key))))
        expected_columns = {"trade_id", "account_id", "quantity", "price"}

        if not rows:
            raise ValueError("Settlement CSV is empty")
        if set(rows[0]) != expected_columns:
            raise ValueError(f"Settlement CSV must contain {sorted(expected_columns)}")

        print(f"Validated {len(rows)} settlement rows")
        return {"record_count": len(rows), "bucket": bucket, "input_key": input_key}

    @task
    def stage_file(validation: dict) -> int:
        """Copy the small CSV into the Oracle staging table."""
        rows = list(
            csv.DictReader(
                StringIO(read_s3_text(validation["bucket"], validation["input_key"]))
            )
        )
        run_id = get_current_context()["run_id"]

        connection = get_oracle_hook().get_conn()
        cursor = connection.cursor()
        cursor.execute("DELETE FROM SETTLEMENT_STAGE_DEMO")
        cursor.executemany(
            """
            INSERT INTO SETTLEMENT_STAGE_DEMO (
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
            "UPDATE SETTLEMENT_STAGE_DEMO SET NOTIONAL = QUANTITY * PRICE"
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
            "- Input: `{{ params.input_key }}`\n\n"
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
        cursor.execute("DELETE FROM SETTLEMENTS_DEMO")
        cursor.execute(
            """
            INSERT INTO SETTLEMENTS_DEMO (
                TRADE_ID, ACCOUNT_ID, QUANTITY, PRICE, NOTIONAL, LOADED_AT
            )
            SELECT TRADE_ID, ACCOUNT_ID, QUANTITY, PRICE, NOTIONAL, SYSTIMESTAMP
            FROM SETTLEMENT_STAGE_DEMO
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
            "SELECT COUNT(*), NVL(SUM(NOTIONAL), 0) FROM SETTLEMENT_STAGE_DEMO"
        )
        stage_count, stage_total = cursor.fetchone()
        cursor.execute(
            "SELECT COUNT(*), NVL(SUM(NOTIONAL), 0) FROM SETTLEMENTS_DEMO"
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

    tables_ready = create_demo_tables()
    validation = validate_file(
        "{{ var.value.jefferies_settlement_bucket }}",
        "{{ params.input_key }}",
    )
    staged = stage_file(validation)
    transformed = transform_rows(staged)
    loaded = load_settlements()
    reconciled = reconcile(loaded)
    complete = stream_complete(reconciled)
    published = publish_completion(complete)

    # These explicit dependencies make the demo read like the batch plan.
    tables_ready >> wait_for_files >> validation
    transformed >> release_or_ice >> loaded
    reconciled >> complete >> published


jefferies_post_market_settlement_etl()
