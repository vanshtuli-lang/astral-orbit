"""Loads the dag-factory flavor of the Barclays ingestion Airflow DAGs from YAML."""

import os
from pathlib import Path

from dagfactory import load_yaml_dags

CONFIG_DIR = Path(__file__).resolve().parent / "barclays_dagfactory"

# The YAML references task callables by absolute file path via this variable, because
# dags/ is not on sys.path during `astro deploy` parse tests.
os.environ["BARCLAYS_DAGFACTORY_DIR"] = str(CONFIG_DIR)

load_yaml_dags(
    globals_dict=globals(),
    config_filepath=str(CONFIG_DIR / "Barclays_ingestion_dagfactory.yml"),
)
