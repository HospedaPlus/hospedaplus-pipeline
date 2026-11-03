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


def test_compose_enables_cron_data_intervals():
    """Incremental DAGs read [data_interval_start, data_interval_end). Airflow 3 makes that
    window empty for cron schedules unless create_cron_data_intervals is on."""
    root = DAGS.parent
    for f in ("docker-compose.yaml", "docker-compose.nucleo.yaml", "docker-compose.workers.yaml"):
        assert 'AIRFLOW__SCHEDULER__CREATE_CRON_DATA_INTERVALS: "True"' in (root / f).read_text(), f


def test_hourly_window_is_one_hour(monkeypatch):
    """With the compose setting, the 05:00 run of an hourly DAG reads 04:00-05:00."""
    import pendulum
    from airflow.timetables.base import TimeRestriction

    monkeypatch.setenv("AIRFLOW__SCHEDULER__CREATE_CRON_DATA_INTERVALS", "True")
    dag = DagBag(dag_folder=str(DAGS), include_examples=False).dags["bronze_neon_f2_f3"]
    info = dag.timetable.next_dagrun_info(
        last_automated_data_interval=None,
        restriction=TimeRestriction(pendulum.datetime(2026, 11, 3, 4, tz="UTC"), None, False),
    )
    assert info.data_interval.end - info.data_interval.start == pendulum.duration(hours=1)
