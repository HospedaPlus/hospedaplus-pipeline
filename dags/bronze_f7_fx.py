"""F7 Frankfurter v2 -> S3 bronze/f7_fx/fecha=YYYY-MM-DD.json, daily at 01:00 UTC."""

from __future__ import annotations

import pendulum
import requests
from airflow.sdk import dag, task

from hospedaplus.lake import BRONZE_F7_FX, put

QUOTES = "COP,EUR,MXN,BRL"


@dag(schedule="0 1 * * *", start_date=pendulum.datetime(2026, 10, 1, tz="UTC"), catchup=False,
     tags=["bronze", "F7"], default_args={"retries": 3, "retry_exponential_backoff": True})
def bronze_f7_fx():
    @task(queue="bronze", outlets=[BRONZE_F7_FX])
    def extract(data_interval_start=None) -> str:
        day = data_interval_start.date().isoformat()
        r = requests.get("https://api.frankfurter.dev/v2/rates",
                         params={"date": day, "base": "USD", "quotes": QUOTES}, timeout=30)
        r.raise_for_status()
        return put(f"bronze/f7_fx/fecha={day}.json", r.content)

    extract()


bronze_f7_fx()
