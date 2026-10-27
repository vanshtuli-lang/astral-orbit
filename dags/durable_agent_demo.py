"""
### Minimal demo: durable=True on @task.agent

One agent, two tools:
1. **roll_dice**   - returns a RANDOM number. Succeeds on every try.
2. **save_roll**   - crashes on try 1 (after roll_dice already ran), works on try 2.

What to look for in the logs of try 2:
- No ">>> LIVE CALL: roll_dice" line -> the tool result came from the cache.
- save_roll prints the SAME number try 1 rolled -> nothing was re-run.
- "Durable: replayed 3 cached steps (2 model, 1 tool), executed ..." summary line.

LLM calls: 3 in total (2 on try 1, 1 on try 2). Without durable it would be 5.
Uses the `pydanticai_default` connection.
"""

import random
from datetime import timedelta

from airflow.sdk import dag, get_current_context, task
from pendulum import datetime
from pydantic_ai.toolsets import FunctionToolset
from pydantic_ai.usage import UsageLimits


def try_number() -> int:
    return get_current_context()["ti"].try_number



def roll_dice() -> int:
    """Roll a 1,000,000-sided die."""
    value = random.randint(1, 1_000_000)
    print(f">>> LIVE CALL on try {try_number()}: roll_dice -> {value}")
    return value


def save_roll(value: int) -> str:
    """Save the rolled value."""
    print(f">>> LIVE CALL on try {try_number()}: save_roll({value})")
    if try_number() == 1:
        raise ConnectionError("Simulated crash after roll_dice succeeded")
    return f"Saved {value}"


@dag(
    start_date=datetime(2026, 10, 1),
    schedule=None,
    tags=["ai", "demo", "durable"],
)
def durable_agent_demo():

    @task.agent(
        llm_conn_id="pydanticai_default",
        system_prompt=(
            "Call roll_dice once. Then call save_roll once with that value. "
            "Then reply with only the number."
        ),
        toolsets=[FunctionToolset(tools=[roll_dice, save_roll])],
        usage_limits=UsageLimits(request_limit=4, total_tokens_limit=4_000),
        durable=True,
        retries=1,
        retry_delay=timedelta(seconds=5),
    )
    def dice_agent() -> str:
        return "Go."

    dice_agent()


durable_agent_demo()
