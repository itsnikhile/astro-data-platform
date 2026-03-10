"""
data_ingestion_dag.py
---------------------
Simulates a data ingestion pipeline that loads raw event data
into a Postgres staging table. On completion, it triggers the
downstream dbt transformation DAG.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email_on_retry": False,
}


def create_raw_table(**context):
    """Create the raw events staging table if it doesn't exist."""
    hook = PostgresHook(postgres_conn_id="postgres_default")
    hook.run("""
        CREATE TABLE IF NOT EXISTS raw.events (
            event_id        VARCHAR(36) PRIMARY KEY,
            user_id         VARCHAR(36) NOT NULL,
            event_type      VARCHAR(50) NOT NULL,
            event_ts        TIMESTAMP   NOT NULL,
            properties      JSONB,
            _loaded_at      TIMESTAMP   DEFAULT NOW()
        );

        CREATE SCHEMA IF NOT EXISTS raw;
    """)


def ingest_events(**context):
    """
    Simulate ingesting event records from an upstream source.
    In production this would call an API, read from S3, Kafka, etc.
    """
    hook = PostgresHook(postgres_conn_id="postgres_default")

    # Simulate a batch of incoming events
    sample_events = [
        ("evt-001", "usr-101", "page_view",  "2024-01-15 08:00:00", '{"page": "/home"}'),
        ("evt-002", "usr-102", "click",       "2024-01-15 08:05:00", '{"element": "cta_button"}'),
        ("evt-003", "usr-101", "purchase",    "2024-01-15 08:10:00", '{"amount": 49.99, "item": "pro_plan"}'),
        ("evt-004", "usr-103", "page_view",   "2024-01-15 08:15:00", '{"page": "/pricing"}'),
        ("evt-005", "usr-104", "signup",      "2024-01-15 08:20:00", '{"source": "organic"}'),
    ]

    insert_sql = """
        INSERT INTO raw.events (event_id, user_id, event_type, event_ts, properties)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (event_id) DO NOTHING;
    """
    hook.run(insert_sql, parameters=sample_events)
    print(f"Ingested {len(sample_events)} events into raw.events")


def validate_ingestion(**context):
    """Basic row count check — fail fast if nothing landed."""
    hook = PostgresHook(postgres_conn_id="postgres_default")
    result = hook.get_first("SELECT COUNT(*) FROM raw.events WHERE _loaded_at >= NOW() - INTERVAL '1 hour'")
    row_count = result[0]

    if row_count == 0:
        raise ValueError("Ingestion validation failed: 0 rows loaded in the last hour.")
    print(f"Validation passed: {row_count} rows loaded.")


with DAG(
    dag_id="data_ingestion_dag",
    default_args=default_args,
    description="Ingest raw events and trigger dbt transformations",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ingestion", "raw", "events"],
) as dag:

    create_table = PythonOperator(
        task_id="create_raw_table",
        python_callable=create_raw_table,
    )

    ingest = PythonOperator(
        task_id="ingest_events",
        python_callable=ingest_events,
    )

    validate = PythonOperator(
        task_id="validate_ingestion",
        python_callable=validate_ingestion,
    )

    trigger_dbt = TriggerDagRunOperator(
        task_id="trigger_dbt_transformations",
        trigger_dag_id="dbt_transformations_dag",
        wait_for_completion=True,
        poke_interval=30,
        reset_dag_run=True,
    )

    create_table >> ingest >> validate >> trigger_dbt
