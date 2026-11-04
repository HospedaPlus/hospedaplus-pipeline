"""Silver: reservas (every channel, newest version per code, amounts in COP),
cancelaciones (base for P1) and reservas_rechazadas (defects refused by Neon).

Runs every day at 05:00 UTC, after bronze_neon_dims (03:15) and the FX silver.
"""

from __future__ import annotations

import pendulum
from airflow.sdk import dag, task

from hospedaplus.lake import SILVER_CANCELACIONES, SILVER_RESERVAS


@dag(schedule="0 5 * * *", start_date=pendulum.datetime(2026, 11, 1, tz="UTC"), catchup=False,
     tags=["silver", "F2", "F3", "F4"])
def silver_reservas_daily():
    @task(queue="silver", outlets=[SILVER_RESERVAS, SILVER_CANCELACIONES])
    def build() -> dict:
        from spark_jobs.silver_reservas import run
        return run()

    build()


silver_reservas_daily()
