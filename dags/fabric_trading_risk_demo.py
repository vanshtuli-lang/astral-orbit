"""Fabric trading risk demo (SIMULATED).

End-of-day trading risk pipeline: Oracle -> OneLake -> Fabric notebooks -> risk summary.

NOTE: this DAG is a simulation for demos. No task connects to Oracle, OneLake or
Microsoft Fabric. Each operator below logs what the real operator would log, pushes
the same XComs, and always succeeds within a few seconds. Swap these classes for the
real providers (SQLCheckOperator, AzureDataLakeStorageV2Hook, MSFabricRunJobOperator)
to run it for real; see git history for the working-integration version.

1. SQLCheckOperator                       - data quality gate on today's Oracle trades
2. OracleToOneLakeOperator                - Oracle -> Parquet -> OneLake (Files/raw)
3. MSFabricRunJobOperator_silver_trades   - Fabric notebook nb_silver_trades
4. MSFabricRunJobOperator_risk_report     - Fabric notebook nb_risk_report
5. OneLakeRiskSummaryOperator             - read summary.json back and log it
"""

import random
import time
import uuid

from airflow.sdk import BaseOperator, CronTriggerTimetable, Variable, dag
from pendulum import datetime, now

# Real IDs are used in the logs when the fabric_demo Variable exists; otherwise
# stable placeholder GUIDs. The Variable is optional so the DAG never fails on it.
DEFAULT_CFG = {
    "workspace_id": "64d7430f-089a-4561-ada4-e5e9e7c1136f",
    "workspace_name": "airflow-finance-demo",
    "lakehouse": "finance_lh",
    "silver_notebook_id": "6f64d433-1f48-4169-b5a8-a6b5d4550177",
    "risk_notebook_id": "3b1c2e7a-5d40-4f6e-9a8b-2c7d1e0f4a19",
}
FABRIC_API = "https://api.fabric.microsoft.com/v1"
FABRIC_APP = "https://app.fabric.microsoft.com"


def fabric_cfg() -> dict:
    try:
        return {**DEFAULT_CFG, **Variable.get("fabric_demo", default={}, deserialize_json=True)}
    except Exception:
        return DEFAULT_CFG


def day_rng(ds: str, salt: str = "") -> random.Random:
    """Same numbers on every rerun of a given day, so the demo is consistent."""
    return random.Random(f"{ds}{salt}")


class SQLCheckOperator(BaseOperator):
    ui_color = "#fff7e6"

    def __init__(self, *, conn_id: str, table: str, **kwargs):
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.table = table

    def execute(self, context):
        ds = context["ds"]
        self.log.info("Using connection ID '%s' for task execution.", self.conn_id)
        self.log.info(
            "Executing SQL check: SELECT has_rows, no_null_price, quantity_positive FROM %s "
            "WHERE trade_date = DATE '%s'", self.table, ds,
        )
        time.sleep(2)
        self.log.info("Record: [1, 1, 1]")
        self.log.info("Success.")


class OracleToOneLakeOperator(BaseOperator):
    ui_color = "#e8f4fd"

    def __init__(self, *, oracle_conn_id: str, onelake_conn_id: str, **kwargs):
        super().__init__(**kwargs)
        self.oracle_conn_id = oracle_conn_id
        self.onelake_conn_id = onelake_conn_id

    def execute(self, context):
        ds, cfg = context["ds"], fabric_cfg()
        rows = 100
        target = f"{cfg['lakehouse']}.Lakehouse/Files/raw/trades/{ds}.parquet"
        self.log.info("Using connection ID '%s' for task execution.", self.oracle_conn_id)
        self.log.info("Fetched %d rows from DEMO_TRADES for trade_date=%s", rows, ds)
        time.sleep(2)
        self.log.info("Wrote %d rows to Parquet (%.1f KiB)", rows, 7.4)
        self.log.info("Using connection ID '%s' for task execution.", self.onelake_conn_id)
        self.log.info(
            "Request URL: 'https://onelake.dfs.fabric.microsoft.com/%s/%s?resource=file' "
            "Request method: 'PUT' Response status: 201", cfg["workspace_name"], target,
        )
        time.sleep(1)
        self.log.info("Uploaded %d trades to %s/%s", rows, cfg["workspace_name"], target)
        return target


class MSFabricRunJobOperator(BaseOperator):
    ui_color = "#e6f6ef"

    def __init__(self, *, fabric_conn_id: str, notebook_key: str, item_name: str,
                 job_type: str = "RunNotebook", **kwargs):
        super().__init__(**kwargs)
        self.fabric_conn_id = fabric_conn_id
        self.notebook_key = notebook_key
        self.item_name = item_name
        self.job_type = job_type

    def execute(self, context):
        ds, cfg, ti = context["ds"], fabric_cfg(), context["ti"]
        ws, item = cfg["workspace_id"], cfg[self.notebook_key]
        run_id, request_id = str(uuid.uuid4()), str(uuid.uuid4())
        location = f"{FABRIC_API}/workspaces/{ws}/items/{item}/jobs/instances/{run_id}"
        link = f"{FABRIC_APP}/groups/{ws}/synapsenotebooks/{item}?experience=fabric-developer"

        self.log.info("Starting Fabric item run - workspace_id: %s, job_type: %s, item_id: %s",
                      ws, self.job_type, item)
        self.log.info("Successfully initialized MSFabricRunJobHook with connection_id: %s, "
                      "poll_interval_seconds: 30, timeout_seconds: 3600, api_host: "
                      "https://api.fabric.microsoft.com", self.fabric_conn_id)
        self.log.info("Job parameters: {\"run_date\": {\"value\": \"%s\", \"type\": \"string\"}}", ds)
        self.log.info("HTTP POST %s/workspaces/%s/items/%s/jobs/Execute/instances "
                      "[status=202, job_id=%s, attempt=1/5]", FABRIC_API, ws, item, run_id)
        self.log.info("Successfully started item run - name: %s, run_id: %s, request_id: %s, "
                      "location: %s", self.item_name, run_id, request_id, location)
        self.log.info("Generated deep link: %s", link)

        for key, value in {
            "workspace_id": ws, "item_id": item, "item_type": self.job_type,
            "run_id": run_id, "item_name": self.item_name, "run_link": link,
        }.items():
            ti.xcom_push(key=key, value=value)

        start = now("UTC")
        for status in ("NotStarted", "InProgress", "InProgress", "Completed"):
            time.sleep(3)
            self.log.info("HTTP GET %s [status=200, attempt=1/5]", location)
            self.log.info("Run status - item_name: %s, run_id: %s, status: %s",
                          self.item_name, run_id, status)

        duration = (now("UTC") - start).in_seconds()
        self.log.info("Run completed - workspace_id: %s, item_id: %s, run_id: %s, status: "
                      "MSFabricRunItemStatus.COMPLETED, duration: %ss", ws, item, run_id, duration)
        return {"run_id": run_id, "status": "Completed", "item_name": self.item_name}


class OneLakeRiskSummaryOperator(BaseOperator):
    ui_color = "#f3e8fd"

    def __init__(self, *, onelake_conn_id: str, **kwargs):
        super().__init__(**kwargs)
        self.onelake_conn_id = onelake_conn_id

    def execute(self, context):
        ds, cfg = context["ds"], fabric_cfg()
        path = f"{cfg['lakehouse']}.Lakehouse/Files/reports/{ds}/summary.json"
        self.log.info("Using connection ID '%s' for task execution.", self.onelake_conn_id)
        self.log.info("Downloading %s/%s", cfg["workspace_name"], path)
        time.sleep(1)

        rng = day_rng(ds)
        desks = ["Rates", "Credit", "Equities", "FX"]
        symbols = ["UST10Y", "AAPL", "MSFT", "EURUSD", "CDX.IG", "JPM", "NVDA"]
        summary = {
            "run_date": ds,
            "trades": 100,
            "gross_notional": round(rng.uniform(38_000_000, 62_000_000), 2),
            "top_position": f"{rng.choice(desks)} / {rng.choice(symbols)}",
        }
        self.log.info(
            "\n===== Trading Risk Summary =====\n"
            f"  Run date       : {summary['run_date']}\n"
            f"  Trades         : {summary['trades']}\n"
            f"  Gross notional : {summary['gross_notional']:,.2f}\n"
            f"  Top position   : {summary['top_position']}\n"
            "================================"
        )
        return summary


@dag(
    dag_id="fabric_trading_risk_demo",
    description="Oracle trades -> OneLake -> Fabric notebooks -> risk summary",
    start_date=datetime(2026, 11, 1, tz="America/New_York"),
    # Weekdays at 6pm New York time, after the US market close.
    schedule=CronTriggerTimetable("0 18 * * 1-5", timezone="America/New_York"),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "trading-risk", "retries": 0},
    tags=["demo", "fabric", "onelake", "oracle", "risk"],
    doc_md=__doc__,
)
def fabric_trading_risk_demo():
    (
        SQLCheckOperator(
            task_id="SQLCheckOperator",
            conn_id="oracle_jefferies_settlement",
            table="DEMO_TRADES",
        )
        >> OracleToOneLakeOperator(
            task_id="OracleToOneLakeOperator",
            oracle_conn_id="oracle_jefferies_settlement",
            onelake_conn_id="onelake_default",
        )
        >> MSFabricRunJobOperator(
            task_id="MSFabricRunJobOperator_silver_trades",
            fabric_conn_id="fabric_default",
            notebook_key="silver_notebook_id",
            item_name="nb_silver_trades",
        )
        >> MSFabricRunJobOperator(
            task_id="MSFabricRunJobOperator_risk_report",
            fabric_conn_id="fabric_default",
            notebook_key="risk_notebook_id",
            item_name="nb_risk_report",
        )
        >> OneLakeRiskSummaryOperator(
            task_id="OneLakeRiskSummaryOperator",
            onelake_conn_id="onelake_default",
        )
    )


fabric_trading_risk_demo()
