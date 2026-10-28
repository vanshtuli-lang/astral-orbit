"""Demo: pick up Barclays settlement files from SFTP as they land.

Files show up on SFTP a few times a day at no fixed time, so there's no cron
here. The DAG runs @continuous - as soon as a run finishes the next one
starts - so a file gets picked up seconds after it lands.

Each file we finish gets written to a Variable (the watermark). The next run
only looks at files sorting after it. So if file 3 of 5 fails, 1 and 2 stay
done and the retry starts at 3.

Nothing is written to Oracle - that part is an EmptyOperator. Files live in
Barclays_ingestion/incoming/ and are named so alphabetical order == arrival
order, e.g. settlement_2026-10-26T0915.csv.
"""

from __future__ import annotations

import logging

import pendulum
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.sftp.hooks.sftp import SFTPHook

SFTP_CONN_ID = "sftp_claims"
ORACLE_CONN_ID = "oracle_jefferies_settlement"  # not used, the load is a stub

INCOMING_DIR = "Barclays_ingestion/incoming"
WATERMARK_VAR = "barclays_sftp_watermark"

log = logging.getLogger(__name__)


@dag(
    dag_id="Barclays_sftp_to_oracle_continuous",
    description="Demo: continuous SFTP pickup with a watermark to resume from",
    start_date=pendulum.datetime(2026, 10, 1, tz="America/New_York"),
    schedule="@continuous",  # no schedule to speak of, just keep looking
    catchup=False,
    max_active_runs=1,  # @continuous needs this, and it keeps runs from racing
    default_args={"owner": "edp-ingestion", "retries": 2},
    tags=["barclays", "sftp", "continuous", "demo"],
    doc_md=__doc__,
)
def barclays_sftp_to_oracle_continuous():

    @task.short_circuit
    def list_new_files() -> list[str]:
        watermark = Variable.get(WATERMARK_VAR, default_var="")
        hook = SFTPHook(ssh_conn_id=SFTP_CONN_ID)

        # Filenames sort by arrival time, so "after the watermark" == "new".
        files = sorted(
            f for f in hook.list_directory(INCOMING_DIR)
            if f.endswith(".csv") and f > watermark
        )

        log.info("watermark=%s, new files=%s", watermark or "(none)", files)
        # Empty list short-circuits: no new files, run ends here.
        return files

    @task
    def process_files(files: list[str]) -> list[str]:
        hook = SFTPHook(ssh_conn_id=SFTP_CONN_ID)
        done = []

        # One at a time, in order. Matters for the resume story below.
        for name in files:
            contents = hook.read_file(f"{INCOMING_DIR}/{name}")
            log.info("read %s (%s bytes)", name, len(contents))

            # Real pipeline would insert the rows right here.

            # Only now is the file off the to-do list. Crash before this line
            # and the next run picks the same file up again.
            Variable.set(WATERMARK_VAR, name)
            done.append(name)

        return done

    # Stand-in for the Oracle load. The demo is about the SFTP side.
    load_to_oracle = EmptyOperator(task_id="load_to_oracle")

    @task
    def report(done: list[str]) -> None:
        log.info("processed %s file(s): %s", len(done), done)

    files = process_files(list_new_files())
    files >> load_to_oracle >> report(files)


barclays_sftp_to_oracle_continuous()
