"""Plain English in, Oracle SQL out.

    get_schema    --\
                     >-->  generate_sql
    ask_question  --/      (LLM writes the Oracle SQL and logs it,
    (HITL: type a           ready to copy and run in Oracle)
     question)

get_schema reads the table columns from Oracle's data dictionary (names and types only, no
data) and saves them in the asset state store. The store persists across DAG runs, so later
runs reuse the saved schema without touching Oracle. Trigger with ``refresh_schema=True``
to re-read it after a table change. The generated query is never run; the engineer runs it.

Tables (STAGING schema, unchanged):
    TRADE_SETTLEMENTS  - completed settlements (Sept 2026), status SETTLED/FAILED/CANCELLED
    SETTLEMENT_STAGE   - trades awaiting settlement (Oct 2026), MATCHED/UNMATCHED/EXCEPTION

Connections: ``pydanticai_default`` (LLM) and ``oracle_jefferies_settlement`` (schema lookup only).
"""

from datetime import datetime as dt
from datetime import timezone

from airflow.providers.common.ai.operators.llm_sql import LLMSQLQueryOperator
from airflow.providers.standard.operators.hitl import HITLEntryOperator
from airflow.sdk import Asset, Param, dag, task
from pendulum import datetime

ORACLE_CONN_ID = "oracle_jefferies_settlement"
SCHEMA = "STAGING"
TABLES = ["TRADE_SETTLEMENTS", "SETTLEMENT_STAGE"]
SCHEMA_ASSET = Asset(name="rbc_settlement_schema")  # holds the cached schema in its state store

EXAMPLE_QUESTIONS = [
    "Which 5 accounts have the highest total settled notional in CAD, with their trade count?",
    "How many settlements failed for each counterparty, and what was the failed notional in CAD?",
    "Show the open exceptions in the settlement queue, grouped by exception reason.",
    "What share of September settlements were SETTLED, FAILED and CANCELLED?",
    "List the 10 largest unmatched trades in the settlement queue by notional in CAD.",
]


@dag(
    start_date=datetime(2026, 10, 1),
    schedule=None,
    catchup=False,
    tags=["rbc", "ai", "demo"],
    params={"refresh_schema": Param(False, type="boolean", description="Re-read the schema from Oracle")},
)
def rbc_nl_to_sql():

    # 1. SCHEMA: from the asset state store, or from Oracle on the first run / refresh
    @task(inlets=[SCHEMA_ASSET])
    def get_schema(params=None, asset_state_store=None) -> str:
        store = asset_state_store[SCHEMA_ASSET]
        cached = store.get("schema_context")
        if cached and not params["refresh_schema"]:
            print(f"Using schema from the asset state store (saved {store.get('fetched_at')})")
            return cached

        from airflow.providers.oracle.hooks.oracle import OracleHook

        cursor = OracleHook(oracle_conn_id=ORACLE_CONN_ID).get_conn().cursor()
        cursor.execute(
            "SELECT table_name, column_name, data_type FROM all_tab_columns "
            "WHERE owner = :owner AND table_name IN (:t1, :t2) ORDER BY table_name, column_id",
            owner=SCHEMA,
            t1=TABLES[0],
            t2=TABLES[1],
        )
        columns: dict[str, list[str]] = {}
        for table, column, data_type in cursor.fetchall():
            columns.setdefault(f"{SCHEMA}.{table}", []).append(f"{column} {data_type}")
        if not columns:
            raise ValueError(f"No columns found for {TABLES} in schema {SCHEMA}")

        schema_context = "\n\n".join(f"Table: {t}\nColumns: {', '.join(c)}" for t, c in columns.items())
        store.set("schema_context", schema_context)
        store.set("fetched_at", dt.now(tz=timezone.utc).isoformat())
        print(f"Read schema from Oracle and saved it to the asset state store:\n{schema_context}")
        return schema_context

    # 2. HUMAN IN THE LOOP: ask a question in plain English
    ask_question = HITLEntryOperator(
        task_id="ask_question",
        subject="Ask the settlement database a question",
        body=(
            "Type your question in plain English. The AI will write the Oracle SQL for you "
            "to copy and run.\n\n**Examples:**\n" + "\n".join(f"- {q}" for q in EXAMPLE_QUESTIONS)
        ),
        params={"question": Param(EXAMPLE_QUESTIONS[0], type="string")},
    )

    # 3. NL-TO-SQL: the LLM writes a validated SELECT and logs it under "Generated SQL:"
    generate_sql = LLMSQLQueryOperator(
        task_id="generate_sql",
        llm_conn_id="pydanticai_default",
        schema_context="{{ ti.xcom_pull(task_ids='get_schema') }}",
        dialect="oracle",
        system_prompt="Use NOTIONAL_CAD when comparing amounts across currencies.",
        prompt="{{ ti.xcom_pull(task_ids='ask_question')['params_input']['question'] }}",
    )

    [get_schema(), ask_question] >> generate_sql


rbc_nl_to_sql()