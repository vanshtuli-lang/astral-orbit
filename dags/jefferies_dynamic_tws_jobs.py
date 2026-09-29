"""
## Jefferies: Scheduled DAG with per-run dynamic params (IBM TWS migration demo)

- Runs on a fixed schedule: weekdays at 6 PM ET.
- Scheduled runs use the default params below.
- Need something different today? Click **Trigger** and edit the params.
  Only that run changes. Every other scheduled run keeps using the defaults.
- `run_job` is one task definition that fans out with `.expand()`:
  one task instance per job name, however many are passed in.
"""

from airflow.sdk import Param, dag, task
from pendulum import datetime


@dag(
    dag_id="Jefferies_dynamic_tws_job_execution",
    start_date=datetime(2026, 1, 1, tz="America/New_York"),
    schedule="0 18 * * 1-5",  # Weekdays 6 PM ET, like a TWS job stream.
    catchup=False,
    doc_md=__doc__,
    tags=["jefferies", "tws-migration", "demo"],
    # Defaults used by every scheduled run. Editable in the Trigger form for a
    # single manual run, without changing code or the schedule.
    params={
        "job_names": Param(["job_A", "job_B"], type="array", items={"type": "string"}),
        "region": Param("US", type="string"),
    },
)
def jefferies_dynamic_tws_job_execution():
    @task
    def get_job_list(**context) -> list[str]:
        # .expand() needs a task output, not a raw param, so pass the list through.
        return context["params"]["job_names"]

    @task
    def run_job(job_name: str, **context):
        region = context["params"]["region"]
        print(f"Running {job_name} in region {region} ({context['dag_run'].run_type.value} run)")

    # One run_job instance is created per job name at runtime.
    run_job.expand(job_name=get_job_list())


jefferies_dynamic_tws_job_execution()
