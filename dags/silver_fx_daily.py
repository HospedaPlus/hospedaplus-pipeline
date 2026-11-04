"""Silver: tasa_cambio_diaria(fecha, moneda, cop_por_unidad) with every day filled.

Runs by itself every time bronze_f7_fx writes (Airflow asset schedule),
on the silver worker, with Spark.
"""

from __future__ import annotations

import pendulum
from airflow.sdk import dag, task

from hospedaplus.lake import BRONZE_F7_FX, SILVER_FX_DAILY


@dag(schedule=[BRONZE_F7_FX], start_date=pendulum.datetime(2026, 10, 1, tz="UTC"), catchup=False, max_active_runs=1,
     tags=["silver", "F7"],
     default_args={"retries": 2, "retry_delay": pendulum.duration(minutes=10),
                   "execution_timeout": pendulum.duration(minutes=45)})
def silver_fx_daily():
    @task(queue="silver", outlets=[SILVER_FX_DAILY])
    def build() -> int:
        from spark_jobs.silver_fx import run
        return run()

    build()


silver_fx_daily()
