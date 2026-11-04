"""Gold: star model (dim_*, fact_*) and one mart per business question (mart_p1..p5).

Runs by itself once a night, when the four silver tables it needs were refreshed:
silver_ocupacion (nightly snapshot only), silver_calendario, silver_reservas (also hourly,
but the "&" waits for the other three, so gold still runs once a night) and silver_resenas
(03:30 UTC). Around 03:35 UTC = 22:35 COT, long before 06:00 local time in every hotel.
spark_jobs/gold_modelo.py rebuilds every table in full and checks it before writing.
"""

from __future__ import annotations

import pendulum
from airflow.sdk import dag, task

from hospedaplus.lake import (
    GOLD_MODELO,
    GOLD_NEON_CANCEL_RATES,
    SILVER_CALENDARIO,
    SILVER_OCUPACION,
    SILVER_RESENAS,
    SILVER_RESERVAS,
)


@dag(schedule=(SILVER_OCUPACION & SILVER_CALENDARIO & SILVER_RESERVAS & SILVER_RESENAS),
     start_date=pendulum.datetime(2026, 11, 1, tz="UTC"), catchup=False, max_active_runs=1,
     tags=["gold", "P1", "P2", "P3", "P4", "P5"],
     default_args={"retries": 2, "retry_delay": pendulum.duration(minutes=10),
                   "execution_timeout": pendulum.duration(minutes=45)})
def gold_modelo_daily():
    @task(queue="gold", outlets=[GOLD_MODELO, GOLD_NEON_CANCEL_RATES])
    def build() -> dict:
        from spark_jobs.gold_modelo import run
        return run()

    build()


gold_modelo_daily()
