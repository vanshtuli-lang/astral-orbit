"""Loads the dag-factory flavor of the Barclays ingestion Airflow DAGs from YAML."""

from pathlib import Path

from dagfactory import load_yaml_dags

load_yaml_dags(
    globals_dict=globals(),
    config_filepath=str(Path(__file__).parent / "barclays_dagfactory" / "Barclays_ingestion_dagfactory.yml"),
)
