"""Silver: historico_f1 (Kaggle history, base for P1 by deposit type and lead time).
Runs by itself whenever bronze_f1_historical uploads the file (it almost never changes)."""

from __future__ import annotations

import pendulum
from airflow.sdk import dag, task

from hospedaplus.lake import BRONZE_F1_HISTORICAL, SILVER_HISTORICO_F1


@dag(schedule=[BRONZE_F1_HISTORICAL],
     start_date=pendulum.datetime(2026, 11, 1, tz="UTC"), catchup=False, tags=["silver", "F1", "P1"],
     default_args={"retries": 2, "retry_delay": pendulum.duration(minutes=10),
                   "execution_timeout": pendulum.duration(minutes=45)})
def silver_f1_historico():
    @task(queue="silver", outlets=[SILVER_HISTORICO_F1])
    def build():
        from spark_jobs.silver_f1 import run
        return run()

    build()


silver_f1_historico()
