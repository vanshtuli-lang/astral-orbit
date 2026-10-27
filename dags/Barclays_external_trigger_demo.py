"""Demo: start a DAG from outside Airflow (Airflow 2.10).

No DAG produces this dataset and nothing in Airflow polls for it. It only
gets updated from outside, in one of two ways:

1. A POST to the REST API - mainframe, Informatica, a Kafka consumer,
   anything that can make an HTTP call:

   curl -X POST 'http://localhost:8080/api/v1/datasets/events' \
     -H 'Content-Type: application/json' --user 'admin:admin' \
     -d '{"dataset_uri": "mainframe://barclays/eod_batch"}'

2. A human opens the Datasets tab and hits manual update.

Either way, this DAG starts within seconds.
"""

from __future__ import annotations

import logging

import pendulum
from airflow.datasets import Dataset
from airflow.decorators import dag, task

# Nothing in Airflow writes to this. The upstream system owns it.
EOD_BATCH = Dataset("mainframe://barclays/eod_batch")


@dag(
    dag_id="Barclays_external_trigger",
    schedule=[EOD_BATCH],
    start_date=pendulum.datetime(2026, 10, 1, tz="America/New_York"),
    catchup=False,
    tags=["barclays", "datasets", "demo"],
    doc_md=__doc__,
)
def external_trigger():

    @task
    def process_batch():
        logging.info("processing the end of day batch")

    process_batch()


external_trigger()
