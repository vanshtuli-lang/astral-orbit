"""
### durable=True: the double-refund demo

An AI agent refunds a customer $1,200, then emails them. The email fails on the first try,
**after the money has already been sent**, so Airflow retries the task.

The same agent runs twice. The only difference is `durable`:

- **durable=False**: the retry starts over and **refunds the customer again** ($2,400 sent).
- **durable=True**: the retry replays the finished steps from the cache and only re-sends the email ($1,200 sent).

Check the `summary` task log for the result. Trigger a new run to repeat the demo.
"""

import uuid
from datetime import timedelta

from airflow.sdk import Variable, dag, get_current_context, task
from pendulum import datetime
from pydantic_ai.toolsets import FunctionToolset


def _ti():
    return get_current_context()["ti"]


def issue_refund(amount: float) -> str:
    """Send a refund to the customer. Returns the refund ID."""
    ti = _ti()
    refund_id = f"RF-{uuid.uuid4().hex[:6].upper()}"
    print(f">>> MONEY SENT: ${amount:,.0f} refund {refund_id} (try {ti.try_number})")
    refunds = Variable.get(f"refunds_{ti.task_id}", default=[], deserialize_json=True)
    Variable.set(f"refunds_{ti.task_id}", refunds + [refund_id], serialize_json=True)
    return refund_id


def email_customer(refund_id: str) -> str:
    """Email the customer about their refund."""
    ti = _ti()
    if ti.try_number == 1:
        raise ConnectionError("Email server is down. Retrying task...")
    print(f">>> EMAIL SENT for {refund_id} (try {ti.try_number})")
    return "sent"


def refund_agent(durable: bool):
    return task.agent(
        task_id=f"refund_agent_durable_{durable}",
        llm_conn_id="pydanticai_default",
        system_prompt="Refund the customer $1200 with issue_refund, then call email_customer with the refund ID.",
        toolsets=[FunctionToolset([issue_refund, email_customer], id="refund_tools", sequential=True)],
        durable=durable,
        retries=1,
        retry_delay=timedelta(seconds=5),
    )(lambda: "Process the refund.")


@dag(start_date=datetime(2026, 10, 1), schedule=None, doc_md=__doc__, tags=["ai", "demo", "durable"])
def durable_agent_demo():

    @task
    def reset():
        for durable in (False, True):
            Variable.set(f"refunds_refund_agent_durable_{durable}", [], serialize_json=True)

    @task(trigger_rule="all_done")
    def summary():
        print("=" * 60)
        for durable in (False, True):
            refunds = Variable.get(f"refunds_refund_agent_durable_{durable}", deserialize_json=True)
            print(f"durable={durable!s:<5} -> refunded {len(refunds)}x = ${1200 * len(refunds):,}  {refunds}")
        print("=" * 60)

    reset() >> [refund_agent(durable=False)(), refund_agent(durable=True)()] >> summary()


durable_agent_demo()
