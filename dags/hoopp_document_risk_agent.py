"""
### HOOPP demo: turn investment documents into a risk brief

1. **read_documents**       - read the documents. Crashes halfway, the retry picks up where it left off.
2. **summarize_documents**  - an LLM summarizes them. An analyst approves (or edits) it in the UI.
3. **risk_agent**           - an AI agent checks exposure with a tool and writes a risk brief.
4. **save_brief**           - saves the brief and compares it with the previous run.

The first try of steps 1 and 3 fails on purpose to show retries.
LLM calls use the `pydanticai_default` connection. All data is fake.
"""

import random
from datetime import timedelta

from airflow.sdk import Asset, dag, get_current_context, task
from pendulum import datetime
from pydantic_ai.toolsets import FunctionToolset
from pydantic_ai.usage import UsageLimits

RISK_BRIEF = Asset(name="investment_risk_brief")

DOCUMENTS = [
    "Infrastructure memo: toll road traffic down 9%. Debt refinancing due March 2027.",
    "Real estate update: office occupancy fell from 80% to 71%. Industrial fully leased.",
    "Private credit review: three retail borrowers on the watch list.",
    "Equities commentary: overweight technology. Currency hedge cut from 50% to 40%.",
]


def fail_on_first_try(message: str):
    """Demo only: raise an error on the first try so Airflow retries the task."""
    if get_current_context()["ti"].try_number == 1:
        raise ConnectionError(message)


def get_exposure(asset_classes: list[str]) -> str:
    """Look up how much of the fund is in each asset class. Pass all asset classes in one call."""
    fail_on_first_try("Simulated database timeout")
    print(f"Agent asked for exposure on: {asset_classes}")
    lines = []
    for asset_class in asset_classes:
        exposure = round(random.uniform(5, 20), 1)  # fake data: some under, some over the limit
        lines.append(f"{asset_class}: {exposure}% of the fund, limit is 15%")
    print("Tool result:\n" + "\n".join(lines))
    return "\n".join(lines)


@dag(
    start_date=datetime(2026, 10, 1),
    schedule=None,
    tags=["hoopp", "ai", "demo"],
    default_args={"retries": 2, "retry_delay": timedelta(seconds=5)},
)
def hoopp_document_risk_agent():

    # Step 1 - shows: TASK STATE STORE (progress survives a crash)
    @task
    def read_documents(task_state_store=None) -> str:
        done = task_state_store.get("docs_read", default=[])
        print(f"Already read before this try: {len(done)} documents")

        for doc in DOCUMENTS[len(done):]:  # skip what an earlier try already read
            if len(done) == 2:
                fail_on_first_try("Simulated crash halfway through")
            print(f"Reading document {len(done) + 1}")
            done.append(doc)
            task_state_store.set("docs_read", done)  # save progress

        return "\n".join(done)

    # Step 2 - shows: LLM TASK + HUMAN APPROVAL + COST LIMIT
    @task.llm(
        llm_conn_id="pydanticai_default",
        system_prompt="Summarize in 3 bullet points.",
        usage_limits=UsageLimits(request_limit=2, total_tokens_limit=2_000),
        require_approval=True,
        allow_modifications=True,
    )
    def summarize_documents(documents: str) -> str:
        return documents

    # Step 3 - shows: AI AGENT + TOOLS + COST LIMIT
    # durable=True: on retry, the agent reuses LLM calls it already paid for.
    @task.agent(
        llm_conn_id="pydanticai_default",
        system_prompt=(
            "You are a pension fund risk analyst. Call get_exposure ONCE with every asset class "
            "from the summary, then write a short risk brief that flags anything over the limit."
        ),
        toolsets=[FunctionToolset(tools=[get_exposure])],
        usage_limits=UsageLimits(request_limit=5, total_tokens_limit=8_000),
        durable=True,
    )
    def risk_agent(summary: str) -> str:
        return summary

    # Step 4 - shows: ASSET STATE STORE (remembered across runs)
    @task(outlets=[RISK_BRIEF])
    def save_brief(brief: str, asset_state_store=None):
        store = asset_state_store[RISK_BRIEF]
        run_count = store.get("run_count", default=0) + 1
        print(f"Run number: {run_count}")
        print(f"Last run's brief:\n{store.get('last_brief', default='(first run)')}")
        print(f"This run's brief:\n{brief}")
        store.set("last_brief", brief)
        store.set("run_count", run_count)

    # The flow
    documents = read_documents()
    summary = summarize_documents(documents)
    brief = risk_agent(summary)
    save_brief(brief)


hoopp_document_risk_agent()
