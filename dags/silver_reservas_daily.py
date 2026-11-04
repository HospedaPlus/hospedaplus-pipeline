"""Silver: reservas (every channel, newest version per code, amounts in COP),
cancelaciones (base for P1) and reservas_rechazadas (defects refused by Neon).

Runs by itself once the three inputs of the day are fresh: the FX silver
(after bronze_f7_fx, 01:00 UTC), the nightly full snapshot (bronze_neon_full_load,
03:00 UTC) and the catalog copy (bronze_neon_dims, 03:15 UTC).
"""

from __future__ import annotations

import pendulum
from airflow.sdk import dag, task

from hospedaplus.lake import BRONZE_F3_FULL, BRONZE_NEON_DIMS, SILVER_CANCELACIONES, SILVER_FX_DAILY, SILVER_RESERVAS


@dag(schedule=(SILVER_FX_DAILY & BRONZE_F3_FULL & BRONZE_NEON_DIMS),
     start_date=pendulum.datetime(2026, 11, 1, tz="UTC"), catchup=False,
     tags=["silver", "F2", "F3", "F4"])
def silver_reservas_daily():
    @task(queue="silver", outlets=[SILVER_RESERVAS, SILVER_CANCELACIONES])
    def build() -> dict:
        from spark_jobs.silver_reservas import run
        return run()

    build()


silver_reservas_daily()
