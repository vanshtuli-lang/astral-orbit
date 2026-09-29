"""Load the Barclays ingestion DAGs declared in Barclays_ingestion_tenants.yml."""

# The words "airflow" and "dag" must appear in this file, otherwise Airflow's
# DAG discovery safe mode skips it.

from pathlib import Path

from dagfactory import load_yaml_dags

load_yaml_dags(
    globals_dict=globals(),
    config_filepath=str(Path(__file__).resolve().with_name("Barclays_ingestion_tenants.yml")),
)
