"""RBC fraud AI agent that learns from analysts.

    load_lessons  ->  investigate_alert  ->  analyst_review  ->  save_lesson
    (asset state)     (AI agent,             (human in          (asset state)
                       task state)            the loop)

Run it twice. Run 1: the analyst teaches the agent. Run 2: the agent uses the lesson.
All data is fake. LLM calls go through the Astro LLM gateway, configured entirely in
the ``pydanticai_default`` connection (no API keys or URLs in this file):

    AIRFLOW_CONN_PYDANTICAI_DEFAULT='{"conn_type": "pydanticai",
        "host": "https://api.astronomer.io/llm/v1",
        "password": "<Astro Organization API Token>",
        "extra": {"model": "openai:gpt-5.4"}}'
"""

from datetime import timedelta

from airflow.providers.standard.operators.hitl import HITLOperator
from airflow.sdk import Asset, Param, dag, get_current_context, task
from pendulum import datetime
from pydantic_ai.toolsets import FunctionToolset
from pydantic_ai.usage import UsageLimits

AGENT_MEMORY = Asset(name="rbc_fraud_agent_memory")
ALERT = "Card 4520-1187 spent $9,950 CAD at CryptoSwap (Malta). Customer C-1."


def get_customer(customer_id: str) -> dict:
    """Look up an RBC customer's profile."""
    if get_current_context()["ti"].try_number == 1:
        raise ConnectionError("Simulated outage! Airflow will retry the agent.")
    return {"tenure_years": 12, "home": "Toronto", "buys_crypto_regularly": True}


@dag(start_date=datetime(2026, 10, 1), schedule=None, catchup=False, tags=["rbc", "ai", "demo"])
def rbc_ai_fraud_alert_triage():

    # 1. ASSET STATE STORE: the agent's long-term memory, kept across runs
    @task(inlets=[AGENT_MEMORY])
    def load_lessons(asset_state_store=None) -> list:
        lessons = asset_state_store[AGENT_MEMORY].get("lessons", default=[])
        print(f"Lessons learned from analysts: {lessons}")
        return lessons

    # 2. AI AGENT: budget cap + durable execution (task state store)
    @task.agent(
        llm_conn_id="pydanticai_default",  # Astro LLM gateway (see docstring)
        system_prompt=(
            "You are an RBC fraud investigator. Use the tools to gather facts. "
            "Any RBC POLICY lines in the request come from senior fraud analysts and are "
            "mandatory: if a policy applies to this alert, you MUST follow it, even when the "
            "customer profile looks normal. Name the policy you applied. "
            "End with FRAUD or NOT FRAUD and a one-sentence reason."
        ),
        toolsets=[FunctionToolset(tools=[get_customer])],
        usage_limits=UsageLimits(request_limit=5, total_tokens_limit=10_000),  # budget cap
        durable=True,  # saves each LLM call to the task state store, so a retry doesn't pay twice
        retries=1,
        retry_delay=timedelta(seconds=5),
    )
    def investigate_alert(lessons: list) -> str:
        policy = "\n".join(f"RBC POLICY: {lesson}" for lesson in lessons) or "RBC POLICY: none yet."
        return f"Alert: {ALERT}\n\n{policy}"

    # 3. HUMAN IN THE LOOP: analyst reviews the AI and can teach it
    analyst_review = HITLOperator(
        task_id="analyst_review",
        subject="Review the AI's fraud decision",
        body="{{ ti.xcom_pull(task_ids='investigate_alert') }}",
        options=["Agree", "Disagree"],
        params={
            "lesson": Param(
                None,
                type=["null", "string"],
                description="What should the agent learn? (optional)",
            )
        },
    )

    # 4. ASSET STATE STORE: save the lesson so every future run gets it
    @task(outlets=[AGENT_MEMORY])
    def save_lesson(review: dict, asset_state_store=None):
        lesson = review["params_input"].get("lesson")
        if lesson:
            memory = asset_state_store[AGENT_MEMORY]
            memory.set("lessons", memory.get("lessons", default=[]) + [lesson])
            print(f"Saved lesson: {lesson}")

    investigate_alert(load_lessons()) >> analyst_review
    save_lesson(analyst_review.output)


rbc_ai_fraud_alert_triage()


# ---------------------------------------------------------------------------
# DEMO HELPER: wipes the agent's memory. Trigger it before each rehearsal and
# right before the live demo, so Run 1 starts with no lessons.
# ---------------------------------------------------------------------------
@dag(schedule=None, tags=["rbc", "demo"])
def rbc_reset_agent_memory():

    @task(outlets=[AGENT_MEMORY])
    def reset(asset_state_store=None):
        asset_state_store[AGENT_MEMORY].set("lessons", [])
        print("Agent memory wiped.")

    reset()


rbc_reset_agent_memory()
