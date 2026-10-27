"""Demo: scheduling DAGs on data instead of time (Airflow 2.10).

A Dataset is just a name. A task says "I update this name", and any DAG
scheduled on that name runs when the task succeeds. No sensors, no
TriggerDagRunOperator.

In 2.x these are called Datasets. Airflow 3 renamed them to Assets.

Trigger the producer and watch the three consumers go on their own.
"""

from __future__ import annotations

import logging

import pendulum
from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.timetables.datasets import DatasetOrTimeSchedule
from airflow.timetables.trigger import CronTriggerTimetable

log = logging.getLogger(__name__)

# Airflow never opens these - they're labels. scheme://path is just convention.
SETTLED = Dataset("oracle://barclays/card_settlement")
RECONCILED = Dataset("oracle://barclays/reconciled")

defaults = {
    "start_date": pendulum.datetime(2026, 10, 1, tz="America/New_York"),
    "catchup": False,
    "tags": ["barclays", "datasets", "demo"],
}


@dag(dag_id="Barclays_ds_producer", schedule=None, doc_md=__doc__, **defaults)
def producer():
    # outlets is the whole thing: task succeeds -> Airflow fires an event for
    # that dataset -> consumers wake up.
    @task(outlets=[SETTLED])
    def load_settlement():
        log.info("loaded the settlement table")

    @task(outlets=[RECONCILED])
    def reconcile():
        log.info("reconciled it")

    load_settlement() >> reconcile()


@dag(
    dag_id="Barclays_ds_consumer_both",
    schedule=[SETTLED, RECONCILED],  # a list means AND - waits for both
    **defaults,
)
def consumer_both():
    @task
    def run():
        log.info("both tables are fresh")

    run()


@dag(
    dag_id="Barclays_ds_consumer_either",
    schedule=(SETTLED | RECONCILED),  # round brackets when you use | or &
    **defaults,
)
def consumer_either():
    @task
    def run():
        log.info("one of them updated, that's enough")

    run()


@dag(
    dag_id="Barclays_ds_consumer_daily",
    # On the dataset, and at 6am regardless - so the report still goes out
    # on a day when nothing upstream ran.
    schedule=DatasetOrTimeSchedule(
        timetable=CronTriggerTimetable("0 6 * * *", timezone="America/New_York"),
        datasets=(RECONCILED,),
    ),
    **defaults,
)
def consumer_daily():
    @task
    def run():
        log.info("building the daily report")

    run()


producer()
consumer_both()
consumer_either()
consumer_daily()
