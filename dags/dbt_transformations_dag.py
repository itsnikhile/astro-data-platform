"""
dbt_transformations_dag.py
--------------------------
Runs dbt models using Astronomer Cosmos, which renders each dbt
node (model, test, snapshot) as an individual Airflow task for
granular observability, retry control, and dependency tracking.

This DAG is triggered by data_ingestion_dag upon successful load.
"""

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from cosmos import DbtDag, DbtTaskGroup, ProjectConfig, ProfileConfig, RenderConfig, ExecutionConfig
from cosmos.profiles import PostgresUserPasswordProfileMapping
from cosmos.constants import LoadMode

DBT_PROJECT_PATH = Path("/usr/local/airflow/include/dbt")
DBT_PROFILES_PATH = DBT_PROJECT_PATH / "profiles"

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "email_on_failure": True,
}

profile_config = ProfileConfig(
    profile_name="astro_dbt_demo",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="postgres_default",
        profile_args={"schema": "dbt_dev"},
    ),
)

project_config = ProjectConfig(
    dbt_project_path=DBT_PROJECT_PATH,
)

execution_config = ExecutionConfig(
    dbt_executable_path="/usr/local/bin/dbt",
)

# Option A: Full DbtDag — entire dbt project as one DAG
dbt_dag = DbtDag(
    dag_id="dbt_transformations_dag",
    project_config=project_config,
    profile_config=profile_config,
    execution_config=execution_config,
    render_config=RenderConfig(
        load_method=LoadMode.DBT_LS,
        select=["tag:daily"],          # only run models tagged 'daily'
    ),
    schedule=None,                     # triggered by ingestion DAG
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["dbt", "transformations", "cosmos"],
)
