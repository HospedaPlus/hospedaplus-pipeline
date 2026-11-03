"""Every DAG file loads without errors and follows the project rules."""

from pathlib import Path

import pytest
from airflow.models.dagbag import DagBag

DAGS = Path(__file__).resolve().parents[1] / "dags"
QUEUES = {"bronze", "silver", "gold"}


@pytest.fixture(scope="session")
def dagbag():
    return DagBag(dag_folder=str(DAGS), include_examples=False)


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}


def test_every_file_has_a_dag(dagbag):
    files = {p.stem for p in DAGS.glob("*.py")}
    assert files <= set(dagbag.dag_ids), files - set(dagbag.dag_ids)


def test_tags_and_queues(dagbag):
    for dag_id, dag in dagbag.dags.items():
        layer = dag_id.split("_")[0]
        assert layer in QUEUES, dag_id
        assert layer in dag.tags, f"{dag_id} needs tag {layer}"
        for t in dag.tasks:
            assert t.queue == layer, f"{dag_id}.{t.task_id} runs on queue {t.queue}, expected {layer}"


def test_no_catchup_storm(dagbag):
    """A new DAG must not launch hundreds of past runs when it is deployed."""
    for dag_id, dag in dagbag.dags.items():
        assert not dag.catchup, dag_id
