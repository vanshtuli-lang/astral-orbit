"""
Sensors demo (Airflow 3 / Astro Runtime 3.x)

A sensor is just a task that waits for something to be true.
It checks, waits poke_interval seconds, and checks again until it's true
or it hits the timeout.

Three ways a sensor can wait:
  poke        -> holds a worker slot the whole time. Fine for short waits.
  reschedule  -> gives the slot back between checks.
  deferrable  -> hands the wait to the triggerer. Uses no worker at all.

Every sensor here that supports deferrable=True uses it.
The two that don't (SqlSensor and @task.sensor) use reschedule instead.

Add these to your .env before running:
  AIRFLOW_CONN_FS_DEFAULT='{"conn_type": "fs", "extra": {"path": "/usr/local/airflow/include"}}'
  AIRFLOW_CONN_GITHUB_API='{"conn_type": "http", "host": "api.github.com", "schema": "https"}'
  AIRFLOW_CONN_ORACLE_DEFAULT='{"conn_type": "oracle", "host": "my-oracle-host", "port": 1521, "login": "user", "password": "pass", "extra": {"service_name": "ORCLPDB1"}}'

And add to requirements.txt:
  apache-airflow-providers-http
  apache-airflow-providers-oracle

  Helper Info: AIRFLOW_CONN_ORACLE_DEFAULT='{"conn_type": "oracle", "host": "my-oracle-host", "port": 1521, "login": "user", "password": "pass", ...}'
"""

from datetime import timedelta

import pendulum
from airflow.sdk import PokeReturnValue, dag, task
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.standard.sensors.date_time import DateTimeSensor
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow.providers.standard.sensors.filesystem import FileSensor


@dag(
    start_date=pendulum.datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["demo", "sensors"],
)
def sensors_showcase():

    # 1. Wait for a time
    # Use case: don't start loading until the business window opens.
    wait_for_time = DateTimeSensor(
        task_id="wait_for_time",
        target_time="{{ (dag_run.start_date + macros.timedelta(seconds=30)).isoformat() }}",
        deferrable=True,
    )

    # 2. Wait for a file
    # Use case: a partner drops a file at some point in the morning.
    # Always set a timeout - the default is 7 days!
    # Heads up: deferred, the check runs on the triggerer, so the triggerer
    # has to see the same folder. Works locally because include/ is shared.
    # On a deployed Astro environment, use S3KeySensor / GCSObjectExistenceSensor instead.
    @task
    def partner_drops_file():
        # pretend this is the partner, not us
        import time
        time.sleep(20)
        open("/usr/local/airflow/include/orders.csv", "w").write("id,amount\n1,100\n")

    wait_for_file = FileSensor(
        task_id="wait_for_file",
        filepath="orders.csv", #Orders.csv X
        poke_interval=10, # optional (better observability + logging) < overhead (compute). -> worker
        #timeout=600,
        deferrable=True, # worker -> triggerer (compute = -50X)
    )

    # 3. Wait for an API to be up
    # Use case: check the source system is healthy before pulling from it.
    wait_for_api = HttpSensor(
        task_id="wait_for_api",
        http_conn_id="github_api",
        endpoint="zen",
        poke_interval=30,
        timeout=600,
        deferrable=True,
    )

    # 4. Wait for a table to have enough rows (Oracle)
    # Use case: don't start the downstream job until the upstream load
    # has landed all of today's rows.
    # SqlSensor runs the query and passes the first value it gets back
    # to `success`. Return True and the sensor is done.
    # Works with any SQL database - just change the connection.
    # SqlSensor can't defer, so reschedule is the next best thing.
    wait_for_rows = SqlSensor(
        task_id="wait_for_rows",
        conn_id="oracle_default",
        sql="""
                SELECT COUNT(*)
                FROM sales.orders
                WHERE order_date = TO_DATE('{{ ds }}', 'YYYY-MM-DD')
            """,
        success=lambda row_count: row_count >= 1000,
        mode="reschedule",
        poke_interval=60,
        timeout=60 * 60,
    )

    wait_for_rows -> 

    # 5. Write your own sensor with plain Python
    # Use case: anything without a built-in sensor (a job status, a flag in a table...).
    # Every 30 seconds we ask the vendor's API if their job is done.
    # PokeReturnValue answers two things:
    #   is_done     -> True ends the sensor, False means "check again later"
    #   xcom_value  -> what to hand to the next task once it's ready
    # @task.sensor can't defer, so reschedule frees the worker between checks.
    # (Swap the URL for your real vendor endpoint.)
    @task.sensor(poke_interval=30, timeout=3600, mode="reschedule")
    def wait_for_job_status():
        import requests

        response = requests.get("https://api.vendor.com/jobs/latest", timeout=10).json()
        return PokeReturnValue(
            is_done=response["status"] == "DONE",
            xcom_value=response["job_id"],
        )

    # Gets the job_id the sensor passed along, then goes and gets the output
    @task
    def use_job_result(job_id):
        print(f"Picking up results from {job_id}")

    # 6. Wait for another DAG
    # Use case: don't build today's report until yesterday's run finished.
    # soft_fail=True means "skip if it never shows up" instead of failing.
    # (On the very first run there is no yesterday, so this will skip.)
    wait_for_yesterday = ExternalTaskSensor(
        task_id="wait_for_yesterday",
        external_dag_id="sensors_showcase",
        external_task_id="done",
        execution_delta=timedelta(days=1),
        timeout=60,
        soft_fail=True,
        deferrable=True,
    )

    # Runs even if the soft_fail sensor was skipped
    @task(trigger_rule="none_failed")
    def done():
        print("All done!")

    finish = done()
    partner_drops_file()
    wait_for_time >> finish
    wait_for_file >> finish
    wait_for_api >> finish
    wait_for_rows >> finish
    use_job_result(wait_for_job_status()) >> finish
    wait_for_yesterday >> finish


sensors_showcase()