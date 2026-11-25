"""Fabric trading risk demo.

End-of-day trading risk pipeline orchestrated by Airflow, with Microsoft Fabric
doing the heavy lifting:

1. check_source_quality  - data quality gate on today's Oracle trades
2. extract_to_onelake    - Oracle -> Parquet -> OneLake (Lakehouse "Files/raw")
3. build_silver_trades   - Fabric notebook cleans raw trades into a silver table
4. build_risk_report     - Fabric notebook builds the risk report + summary.json
5. publish_risk_summary  - read summary.json back from OneLake and log it

Setup:
- Airflow Variable ``fabric_demo`` (JSON) with keys: workspace_id, workspace_name,
  lakehouse, silver_notebook_id, risk_notebook_id.
- Connections: ``oracle_jefferies_settlement`` (Oracle), ``onelake_default``
  (ADLS Gen2, extra ``account_url`` = https://onelake.dfs.fabric.microsoft.com),
  ``fabric_default`` (Microsoft Fabric service principal).
- Oracle table: run ``include/fabric/demo_trades.sql`` once.
"""

import json
import tempfile

from airflow.providers.common.sql.operators.sql import SQLCheckOperator
from airflow.providers.microsoft.fabric.operators.run_item import (
    MSFabricNotebookJobParameters,
    MSFabricRunJobOperator,
)
from airflow.sdk import CronTriggerTimetable, Variable, dag, task
from pendulum import datetime

# Reuse the existing Oracle connection shared by the other demo DAGs.
ORACLE_CONN_ID = "oracle_jefferies_settlement"
ONELAKE_CONN_ID = "onelake_default"
FABRIC_CONN_ID = "fabric_default"

# Jinja shortcut for operator fields: resolved at run time, not at parse time.
FABRIC_VAR = "var.json.fabric_demo"


def get_onelake_hook():
    """OneLake speaks the ADLS Gen2 API, so the standard Azure hook works."""
    from airflow.providers.microsoft.azure.hooks.data_lake import AzureDataLakeStorageV2Hook

    return AzureDataLakeStorageV2Hook(adls_conn_id=ONELAKE_CONN_ID)


def run_notebook(task_id: str, notebook_key: str) -> MSFabricRunJobOperator:
    """Run a Fabric notebook from the fabric_demo Variable, passing run_date."""
    # job_params is a templated field, so {{ ds }} is rendered at run time.
    params = MSFabricNotebookJobParameters().set_parameter("run_date", "{{ ds }}", "string")
    return MSFabricRunJobOperator(
        task_id=task_id,
        fabric_conn_id=FABRIC_CONN_ID,
        workspace_id="{{ " + FABRIC_VAR + ".workspace_id }}",
        item_id="{{ " + FABRIC_VAR + "." + notebook_key + " }}",
        job_type="RunNotebook",
        job_params=params.to_json(),
        deferrable=True,  # wait on the triggerer, not a worker slot
    )


@dag(
    dag_id="fabric_trading_risk_demo",
    description="Oracle trades -> OneLake -> Fabric notebooks -> risk summary",
    start_date=datetime(2026, 11, 1, tz="America/New_York"),
    # Weekdays at 6pm New York time, after the US market close.
    schedule=CronTriggerTimetable("0 18 * * 1-5", timezone="America/New_York"),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "trading-risk", "retries": 2},
    tags=["demo", "fabric", "onelake", "oracle", "risk"],
    doc_md=__doc__,
)
def fabric_trading_risk_demo():
    # 1. Data quality gate. SQLCheckOperator fails the task if any column in the
    #    single result row is 0. (The column/table check operators wrap their
    #    SQL in "AS sq", which Oracle rejects, so we write the checks directly.)
    check_source_quality = SQLCheckOperator(
        task_id="check_source_quality",
        conn_id=ORACLE_CONN_ID,
        sql="""
            SELECT
                -- at least one trade for the day
                CASE WHEN COUNT(*) >= 1 THEN 1 ELSE 0 END AS has_rows,
                -- COUNT(price) skips NULLs, so equal counts means no null prices
                CASE WHEN COUNT(*) = COUNT(price) THEN 1 ELSE 0 END AS no_null_price,
                -- every quantity is present and positive
                CASE WHEN SUM(CASE WHEN quantity IS NULL OR quantity <= 0 THEN 1 ELSE 0 END) = 0
                     THEN 1 ELSE 0 END AS quantity_positive
            FROM DEMO_TRADES
            WHERE trade_date = DATE '{{ ds }}'
        """,
    )

    @task
    def extract_to_onelake(ds: str) -> str:
        """Pull the day's trades from Oracle and land them in OneLake as Parquet."""
        from airflow.providers.oracle.hooks.oracle import OracleHook

        cfg = Variable.get("fabric_demo", deserialize_json=True)

        df = OracleHook(oracle_conn_id=ORACLE_CONN_ID).get_df(
            """
            SELECT trade_id, trade_date, desk, symbol, side, quantity, price
            FROM DEMO_TRADES
            WHERE trade_date = TO_DATE(:run_date, 'YYYY-MM-DD')
            ORDER BY trade_id
            """,
            parameters={"run_date": ds},
        )
        # Oracle returns UPPERCASE column names; Spark/Fabric prefer lowercase.
        df.columns = [c.lower() for c in df.columns]

        # OneLake layout: file system = workspace, path starts at the Lakehouse.
        target = f"{cfg['lakehouse']}.Lakehouse/Files/raw/trades/{ds}.parquet"
        with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
            df.to_parquet(tmp.name, index=False)
            get_onelake_hook().upload_file(
                file_system_name=cfg["workspace_name"],
                file_name=target,
                file_path=tmp.name,
                overwrite=True,  # re-runs replace the day's file
            )

        print(f"Uploaded {len(df)} trades to {cfg['workspace_name']}/{target}")
        return target

    # 3 & 4. Fabric notebooks: raw -> silver trades, then silver -> risk report.
    build_silver_trades = run_notebook("build_silver_trades", "silver_notebook_id")
    build_risk_report = run_notebook("build_risk_report", "risk_notebook_id")

    @task
    def publish_risk_summary(ds: str) -> dict:
        """Read the risk notebook's summary.json from OneLake and log it."""
        cfg = Variable.get("fabric_demo", deserialize_json=True)
        path = f"{cfg['lakehouse']}.Lakehouse/Files/reports/{ds}/summary.json"

        file_client = get_onelake_hook().get_file_system(cfg["workspace_name"]).get_file_client(path)
        summary = json.loads(file_client.download_file().readall())

        print(
            "\n===== Trading Risk Summary =====\n"
            f"  Run date       : {summary.get('run_date', ds)}\n"
            f"  Trades         : {summary.get('trades')}\n"
            f"  Gross notional : {float(summary.get('gross_notional', 0)):,.2f}\n"
            f"  Top position   : {summary.get('top_position')}\n"
            "================================"
        )
        return summary

    (
        check_source_quality
        >> extract_to_onelake()
        >> build_silver_trades
        >> build_risk_report
        >> publish_risk_summary()
    )


fabric_trading_risk_demo()
