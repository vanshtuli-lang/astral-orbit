"""
Self-healing settlement pipeline.

Normal run:   fetch_file -> check_quality -> print_data
On failure:   ask_otto -> write_fix -> open_pr

When a pipeline task fails, Otto investigates the run, an LLM rewrites this
file based on Otto's findings, and a pull request is opened for review.

Deployment env vars: ASTRO_API_TOKEN, GITHUB_REPO (owner/repo), GITHUB_TOKEN.
"""

import base64
import csv
import json
import os
from io import StringIO

import requests
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.sdk import TriggerRule, dag, get_current_context, task
from pendulum import datetime

SFTP_FILE = "jefferies_settlement/incoming/settlement_data.csv"
COLUMNS = ["trade_id", "account_id", "quantity", "price"]
REPO_PATH = "dags/self_healing_settlement_pipeline.py"

# Astro IDs (not secrets). Organization: Cosmic Energy, Deployment: Vansh_Demo_DND.
ASTRO_ORG_ID = "cl17wsdpe084o0rw3e46g3smn"
ASTRO_DEPLOYMENT_ID = "cmuiehb7y3rk701jr0k2e5t7a"


@dag(start_date=datetime(2026, 10, 1), schedule=None, catchup=False, tags=["otto", "demo"])
def self_healing_settlement_pipeline():

    # ---------------- Pipeline ----------------

    @task
    def fetch_file():
        with SFTPHook(ssh_conn_id="sftp_claims").get_managed_conn() as sftp:
            with sftp.open(SFTP_FILE, "r") as f:
                text = f.read().decode("utf-8")
        return list(csv.DictReader(StringIO(text)))

    @task
    def check_quality(rows):
        assert rows, "File is empty"
        assert list(rows[0]) == COLUMNS, f"Expected columns {COLUMNS}, got {list(rows[0])}"
        for row in rows:
            assert row["trade_id"], "Missing trade_id"
            assert int(row["quantity"]) > 0, f"Bad quantity in {row['trade_id']}"
            assert float(row["price"]) > 0, f"Bad price in {row['trade_id']}"
        return rows

    @task
    def print_data(rows):
        for row in rows:
            print(row)

    # ---------------- Self-healing (only runs if a pipeline task fails) ----------------

    @task(trigger_rule=TriggerRule.ONE_FAILED, retries=0)
    def ask_otto():
        ctx = get_current_context()
        url = (
            "https://api.astronomer.io/labs/v1"
            f"/organizations/{ASTRO_ORG_ID}"
            f"/observability/deployments/{ASTRO_DEPLOYMENT_ID}"
            "/dag-failure-diagnosis/runs"
        )
        headers = {"Authorization": f"Bearer {os.environ['ASTRO_API_TOKEN']}"}

        # Start the investigation.
        start = requests.post(
            url, headers=headers, timeout=30,
            json={"dagId": ctx["dag"].dag_id, "runId": ctx["run_id"]},
        )
        start.raise_for_status()

        # Otto streams progress events. We wait for the one with the final diagnosis.
        events = requests.get(
            f"{url}/{start.json()['runId']}/events",
            headers=headers, stream=True, timeout=(30, 600),
        )
        event = None
        for line in events.iter_lines(decode_unicode=True):
            if line.startswith("event:"):
                event = line[6:].strip()
            elif line.startswith("data:") and event == "rca_diagnosis":
                diagnosis = json.loads(line[5:])
                print("Root cause:", diagnosis.get("summary"))
                print("Suggested fix:", diagnosis.get("suggested_fix"))
                return diagnosis
        raise RuntimeError("Otto did not return a diagnosis")

    @task.agent(
        llm_conn_id="pydanticai_default",
        system_prompt=(
            "You fix Airflow DAG files. Given a diagnosis and the current file, return the "
            "complete corrected file and nothing else, with the smallest change that fixes "
            "the problem. If the problem is not in the code, return exactly NO_FIX."
        ),
    )
    def write_fix(diagnosis):
        current_code = open(__file__).read()
        return f"Diagnosis:\n{json.dumps(diagnosis, indent=2)}\n\nCurrent file:\n{current_code}"

    @task(retries=0)
    def open_pr(fixed_code, diagnosis):
        # Strip markdown fences in case the model added them.
        fixed_code = fixed_code.strip().removeprefix("```python").removesuffix("```").strip() + "\n"
        if fixed_code.strip() == "NO_FIX":
            print("Not a code problem, so no PR. Otto suggests:", diagnosis.get("suggested_fix"))
            return

        repo = f"https://api.github.com/repos/{os.environ['GITHUB_REPO']}"
        headers = {"Authorization": f"Bearer {os.environ['GITHUB_TOKEN']}"}
        branch = f"otto-fix-{get_current_context()['ts_nodash']}"

        # Create a branch from main.
        main_sha = requests.get(f"{repo}/git/ref/heads/main", headers=headers).json()["object"]["sha"]
        requests.post(f"{repo}/git/refs", headers=headers,
                      json={"ref": f"refs/heads/{branch}", "sha": main_sha}).raise_for_status()

        # Commit the fixed file to the new branch.
        file_sha = requests.get(f"{repo}/contents/{REPO_PATH}?ref=main", headers=headers).json()["sha"]
        requests.put(f"{repo}/contents/{REPO_PATH}", headers=headers, json={
            "message": "Fix pipeline failure (suggested by Otto)",
            "content": base64.b64encode(fixed_code.encode()).decode(),
            "sha": file_sha,
            "branch": branch,
        }).raise_for_status()

        # Open the pull request.
        pr = requests.post(f"{repo}/pulls", headers=headers, json={
            "title": f"[Otto] {diagnosis.get('title', 'Fix pipeline failure')}",
            "head": branch,
            "base": "main",
            "body": f"**Root cause:** {diagnosis.get('summary')}\n\n"
                    f"**Suggested fix:** {diagnosis.get('suggested_fix')}",
        })
        pr.raise_for_status()
        print("Pull request opened:", pr.json()["html_url"])

    # Keeps the run marked as failed, even after the PR is opened.
    @task(trigger_rule=TriggerRule.ONE_FAILED)
    def mark_failed():
        raise RuntimeError("Pipeline failed. Check the Otto pull request.")

    # ---------------- Wiring ----------------

    rows = fetch_file()
    checked = check_quality(rows)
    printed = print_data(checked)
    pipeline = [rows, checked, printed]

    diagnosis = ask_otto()
    open_pr(write_fix(diagnosis), diagnosis)

    pipeline >> diagnosis
    pipeline >> mark_failed()


self_healing_settlement_pipeline()
