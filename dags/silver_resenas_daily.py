"""Silver: resenas (latest version per review, cross-platform duplicates removed), daily
at 03:30 UTC, after the catalog copy (03:15). The hourly F5 bronze would fire it 24
times a day if it were asset-scheduled, so it runs on a clock."""

from __future__ import annotations

import pendulum
from airflow.sdk import dag, task

from hospedaplus.lake import SILVER_RESENAS


@dag(schedule="30 3 * * *",
     start_date=pendulum.datetime(2026, 11, 1, tz="UTC"), catchup=False, max_active_runs=1, tags=["silver", "F5", "P5"],
     default_args={"retries": 2, "retry_delay": pendulum.duration(minutes=10),
                   "execution_timeout": pendulum.duration(minutes=45)})
def silver_resenas_daily():
    @task(queue="silver", outlets=[SILVER_RESENAS])
    def build():
        from spark_jobs.silver_resenas import run
        return run()

    build()


silver_resenas_daily()
