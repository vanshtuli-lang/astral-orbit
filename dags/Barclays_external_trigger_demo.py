"""Demo: start a DAG from outside Airflow (Airflow 3).

No DAG produces this asset and nothing in Airflow polls for it. It only gets
updated from outside, in one of two ways:

1. A POST to the REST API - mainframe, Informatica, a Kafka consumer,
   anything that can make an HTTP call. See include/trigger_eod_batch.sh.

2. A human opens the Assets tab, picks the asset, and hits the play button
   (choose "Manual", not "Materialize" - nothing produces this one).

Either way, this DAG starts within seconds.
"""

from __future__ import annotations

import logging

import pendulum
from airflow.sdk import Asset, dag, task

# Nothing in Airflow writes to this. The upstream system owns it.
EOD_BATCH = Asset("mainframe://barclays/eod_batch")


@dag(
    dag_id="Barclays_external_trigger",
    schedule=[EOD_BATCH],
    start_date=pendulum.datetime(2026, 10, 1, tz="America/New_York"),
    catchup=False,
    tags=["barclays", "assets", "demo"],
    doc_md=__doc__,
)
def external_trigger():

    @task
    def process_batch():
        logging.info("processing the end of day batch")

    process_batch()


external_trigger()
