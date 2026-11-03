"""F6a Nager holidays API v4 -> S3 bronze/f6_holidays/anio=YYYY/pais=XX.json, monthly.

GET {NAGER_BASE_URL}/api/v4/Holidays/{countryCode}/{year}
Current and next year, because bookings are made months ahead.
"""

from __future__ import annotations

import os

import pendulum
import requests
from airflow.sdk import dag, task

from hospedaplus.lake import BRONZE_F6_HOLIDAYS, put

COUNTRIES = ["PT", "CO", "MX", "BR", "ES"]
NAGER_BASE_URL = os.getenv("NAGER_BASE_URL", "https://nagerholidays.com").rstrip("/")


@dag(schedule="0 2 1 * *", start_date=pendulum.datetime(2026, 10, 1, tz="UTC"), catchup=False,
     tags=["bronze", "F6"], default_args={"retries": 3, "retry_exponential_backoff": True})
def bronze_f6_holidays():
    @task(queue="bronze", outlets=[BRONZE_F6_HOLIDAYS])
    def extract(data_interval_start=None) -> list[str]:
        year = data_interval_start.year
        written = []
        for y in (year, year + 1):
            for country in COUNTRIES:
                r = requests.get(f"{NAGER_BASE_URL}/api/v4/Holidays/{country}/{y}", timeout=30)
                r.raise_for_status()
                written.append(put(f"bronze/f6_holidays/anio={y}/pais={country}.json", r.content))
        return written

    extract()


bronze_f6_holidays()
