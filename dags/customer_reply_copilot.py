"""
Customer reply copilot.

A customer writes in about an order. Airflow collects the context the AI needs, the AI
drafts a reply, and a person reviews it. If the reviewer adds a lesson, it is saved and
becomes part of the context on every later run.

    get_order   ──┐
    get_policy  ──┼──> build_context ──> draft_reply ──> review ──> save_feedback
    get_lessons ──┘

Where the context comes from:
    Oracle   NOVA_ORDERS                    facts about this order
    SFTP     customer_copilot/policy.md     company policy
    Airflow  asset state store              lessons from earlier reviews

Run reset_customer_copilot before a demo to clear the lessons.
"""

from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.standard.operators.hitl import HITLOperator
from airflow.sdk import Asset, Param, dag, task
from pendulum import datetime

ORACLE_CONN_ID = "oracle_jefferies_settlement"
SFTP_CONN_ID = "sftp_claims"
LLM_CONN_ID = "pydanticai_default"
POLICY_PATH = "customer_copilot/policy.md"

# The lessons are stored in this asset's state store, so they survive between runs.
LESSONS = Asset(name="customer_copilot_lessons")

ORDER_COLUMNS = [
    "order_id", "customer_name", "customer_tier", "product",
    "order_date", "delivered_date", "status", "amount",
]

SYSTEM_PROMPT = """You write email replies to Nova Retail customers.
Use only the facts in the context. Don't promise anything the policy doesn't allow.
Lessons from past reviews are instructions from our support team and take priority
over your own judgement. Keep the reply short and friendly, and sign it "Nova Customer Care"."""


@dag(
    start_date=datetime(2026, 10, 1),
    schedule=None,
    catchup=False,
    tags=["ai", "context-engineering", "demo"],
    params={
        "order_id": Param(1001, type="integer", description="Order the customer is writing about"),
        "message": Param(
            "Hi, my blender arrived with a cracked jug. Can I get a refund? - Sarah",
            type="string",
            description="The customer's email",
        ),
    },
)
def customer_reply_copilot():

    @task
    def get_order(params=None) -> dict:
        row = OracleHook(oracle_conn_id=ORACLE_CONN_ID).get_first(
            f"SELECT {', '.join(ORDER_COLUMNS)} FROM NOVA_ORDERS WHERE order_id = :order_id",
            parameters={"order_id": params["order_id"]},
        )
        if row is None:
            raise ValueError(f"Order {params['order_id']} not found in NOVA_ORDERS")
        return {col: str(value) if value is not None else "-" for col, value in zip(ORDER_COLUMNS, row)}

    @task
    def get_policy() -> str:
        with SFTPHook(ssh_conn_id=SFTP_CONN_ID).get_managed_conn() as sftp:
            with sftp.open(POLICY_PATH) as f:
                return f.read().decode("utf-8")

    @task(inlets=[LESSONS])
    def get_lessons(asset_state_store=None) -> list[str]:
        return asset_state_store[LESSONS].get("lessons", default=[])

    @task
    def build_context(order: dict, policy: str, lessons: list[str], params=None) -> str:
        order_facts = "\n".join(f"{key}: {value}" for key, value in order.items())
        past_lessons = "\n".join(f"- {lesson}" for lesson in lessons) or "None yet."

        context = (
            f"ORDER (from Oracle)\n{order_facts}\n\n"
            f"COMPANY POLICY (from SFTP)\n{policy}\n\n"
            f"LESSONS FROM PAST REVIEWS (from Airflow)\n{past_lessons}\n\n"
            f"CUSTOMER EMAIL\n{params['message']}"
        )
        # Printed so anyone can open this task's log and see exactly what the AI was given.
        print(context)
        return context

    @task.llm(llm_conn_id=LLM_CONN_ID, system_prompt=SYSTEM_PROMPT, retries=2)
    def draft_reply(context: str) -> str:
        return context

    review = HITLOperator(
        task_id="review",
        subject="Review the AI's reply",
        body=(
            "**Customer email:**\n\n> {{ params.message }}\n\n"
            "**Draft reply:**\n\n{{ ti.xcom_pull(task_ids='draft_reply') }}"
        ),
        options=["Send", "Don't send"],
        params={
            "lesson": Param(
                None,
                type=["null", "string"],
                description="Anything the AI should do differently next time?",
            ),
        },
    )

    @task(outlets=[LESSONS])
    def save_feedback(review_result: dict, asset_state_store=None):
        if review_result["chosen_options"] == ["Send"]:
            print("Reply approved. (Demo only: no email is sent.)")
        else:
            print("Reply rejected.")

        lesson = review_result["params_input"].get("lesson")
        if lesson:
            store = asset_state_store[LESSONS]
            store.set("lessons", store.get("lessons", default=[]) + [lesson])
            print(f"New lesson saved for future replies: {lesson}")

    context = build_context(get_order(), get_policy(), get_lessons())
    draft_reply(context) >> review
    save_feedback(review.output)


customer_reply_copilot()


@dag(start_date=datetime(2026, 10, 1), schedule=None, catchup=False, tags=["ai", "demo"])
def reset_customer_copilot():
    """Clears the saved lessons so the next demo starts from scratch."""

    @task(outlets=[LESSONS])
    def clear_lessons(asset_state_store=None):
        asset_state_store[LESSONS].set("lessons", [])
        print("Lessons cleared.")

    clear_lessons()


reset_customer_copilot()
