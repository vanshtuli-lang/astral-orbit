"""
### HOOPP demo: AI document summary + risk agent

    read_documents  ->  summarize_documents  ->  risk_agent  ->  save_brief
    (task state:        (LLM + human             (AI agent,      (asset state:
     resumes on retry)   approval)                retries)        remembered across runs)

LLM calls use the `pydanticai_default` connection. All documents are fake.
"""

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


def get_exposure(asset_class: str) -> str:
    """Look up how much of the fund is in an asset class."""
    # Fails on the first try on purpose, so you can watch Airflow retry it
    if get_current_context()["ti"].try_number == 1:
        raise ConnectionError("Simulated database timeout")
    return f"{asset_class}: 14% of the fund, limit is 15%"


@dag(
    start_date=datetime(2026, 10, 1),
    schedule=None,
    tags=["hoopp", "ai", "demo"],
    default_args={
        "retries": 2,  # try again before giving up
        "retry_delay": timedelta(seconds=5),
    },
)
def hoopp_document_risk_agent():

    # 1. TASK STATE STORE: save progress after each document. The first try
    # crashes halfway; the retry picks up at document 3 instead of starting over.
    @task
    def read_documents(ti=None, task_state_store=None) -> str:
        done = task_state_store.get("docs_read", default=[])
        print(f"Already read before this try: {len(done)} documents")

        for i, doc in enumerate(DOCUMENTS):
            if i < len(done):
                continue  # read on an earlier try, skip it
            if i == 2 and ti.try_number == 1:
                raise ConnectionError("Simulated crash halfway through")
            print(f"Reading document {i + 1}")
            done.append(doc)
            task_state_store.set("docs_read", done)

        return "\n".join(done)

    # 2. LLM OPERATOR + HUMAN IN THE LOOP + COST GUARDRAIL
    # The task pauses until an analyst approves (and can edit) the summary in
    # the Airflow UI. usage_limits caps how much this one task can spend.
    @task.llm(
        llm_conn_id="pydanticai_default",
        system_prompt="Summarize in 3 bullet points.",
        usage_limits=UsageLimits(request_limit=2, total_tokens_limit=2_000),
        require_approval=True,
        allow_modifications=True,
    )
    def summarize_documents(documents: str) -> str:
        return documents

    # 3. AI AGENT + COST GUARDRAIL: the agent decides which tools to call, but
    # it can't go past 5 LLM calls or 8,000 tokens.
    # durable=True saves each LLM call in the task state store, so when the tool
    # fails and Airflow retries, the agent doesn't pay for those calls again.
    @task.agent(
        llm_conn_id="pydanticai_default",
        system_prompt="You are a pension fund risk analyst. Check exposure with the tool, then write a short risk brief.",
        toolsets=[FunctionToolset(tools=[get_exposure])],
        usage_limits=UsageLimits(request_limit=5, total_tokens_limit=8_000),
        durable=True,
    )
    def risk_agent(summary: str) -> str:
        return summary

    # 4. ASSET STATE STORE: remembers the last brief and a run count across
    # runs, so each run can be compared with the one before it.
    @task(outlets=[RISK_BRIEF])
    def save_brief(brief: str, asset_state_store=None):
        store = asset_state_store[RISK_BRIEF]
        run_count = store.get("run_count", default=0) + 1
        print(f"Run number: {run_count}")
        print(f"Last run's brief:\n{store.get('last_brief', default='(first run)')}")
        print(f"This run's brief:\n{brief}")
        store.set("last_brief", brief)
        store.set("run_count", run_count)

    save_brief(risk_agent(summarize_documents(read_documents())))


hoopp_document_risk_agent()
