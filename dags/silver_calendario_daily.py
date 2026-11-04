"""Silver: calendario_hotel (hotel x day: public holiday, local events, weekend).
Runs by itself after the daily events snapshot and the catalog copy; and also as soon as
bronze_f6_holidays writes new holidays (monthly), without waiting for the next night."""

from __future__ import annotations

import pendulum
from airflow.sdk import dag, task

from hospedaplus.lake import BRONZE_F6_EVENTS, BRONZE_F6_HOLIDAYS, BRONZE_NEON_DIMS, SILVER_CALENDARIO


@dag(schedule=((BRONZE_F6_EVENTS & BRONZE_NEON_DIMS) | BRONZE_F6_HOLIDAYS),
     start_date=pendulum.datetime(2026, 11, 1, tz="UTC"), catchup=False, max_active_runs=1, tags=["silver", "F6"],
     default_args={"retries": 2, "retry_delay": pendulum.duration(minutes=10),
                   "execution_timeout": pendulum.duration(minutes=45)})
def silver_calendario_daily():
    @task(queue="silver", outlets=[SILVER_CALENDARIO])
    def build():
        from spark_jobs.silver_calendario import run
        return run()

    build()


silver_calendario_daily()
