"""Silver: ocupacion_diaria (hotel x room type x night: rooms, sold, overbooking limit,
relocations, refused). Runs by itself after the nightly Neon snapshot and the catalog copy."""

from __future__ import annotations

import pendulum
from airflow.sdk import dag, task

from hospedaplus.lake import BRONZE_F3_FULL, BRONZE_NEON_DIMS, SILVER_OCUPACION


@dag(schedule=(BRONZE_F3_FULL & BRONZE_NEON_DIMS),
     start_date=pendulum.datetime(2026, 11, 1, tz="UTC"), catchup=False, max_active_runs=1,
     tags=["silver", "F3", "P2", "P3"],
     default_args={"retries": 2, "retry_delay": pendulum.duration(minutes=10),
                   "execution_timeout": pendulum.duration(minutes=45)})
def silver_ocupacion_daily():
    @task(queue="silver", outlets=[SILVER_OCUPACION])
    def build():
        from spark_jobs.silver_ocupacion import run
        return run()

    build()


silver_ocupacion_daily()
