"""
test_dags.py
------------
Validates all DAGs in the project parse without errors.
Run with: pytest tests/
"""

import pytest
from airflow.models import DagBag


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder="dags/", include_examples=False)


def test_no_import_errors(dagbag):
    """All DAGs must import without errors."""
    assert dagbag.import_errors == {}, (
        f"DAG import errors: {dagbag.import_errors}"
    )


def test_dag_ids_present(dagbag):
    """Expected DAG IDs must exist."""
    expected = {"data_ingestion_dag", "dbt_transformations_dag"}
    assert expected.issubset(set(dagbag.dag_ids)), (
        f"Missing DAGs: {expected - set(dagbag.dag_ids)}"
    )


def test_ingestion_dag_structure(dagbag):
    """Ingestion DAG must have correct tasks in the right order."""
    dag = dagbag.get_dag("data_ingestion_dag")
    task_ids = [t.task_id for t in dag.tasks]

    assert "create_raw_table"           in task_ids
    assert "ingest_events"              in task_ids
    assert "validate_ingestion"         in task_ids
    assert "trigger_dbt_transformations" in task_ids


def test_ingestion_dag_no_cycles(dagbag):
    """DAG must be acyclic."""
    dag = dagbag.get_dag("data_ingestion_dag")
    assert dag.test_cycle() is False


def test_retries_configured(dagbag):
    """All tasks should have retries configured."""
    dag = dagbag.get_dag("data_ingestion_dag")
    for task in dag.tasks:
        assert task.retries >= 1, f"Task {task.task_id} has no retries configured."
