"""F6b local events (team CSV, kept in Neon operation.local_events) -> S3 bronze, daily.

The table is small reference data, so each run writes a full snapshot:
bronze/f6_events/fecha=YYYY-MM-DD.jsonl
"""

from __future__ import annotations

import pendulum
from airflow.sdk import dag, task

from hospedaplus.lake import BRONZE_F6_EVENTS, put
from hospedaplus.neon import dump

SQL = "SELECT hotel_code, name, start_date, end_date, category FROM operation.local_events ORDER BY start_date"


@dag(schedule="30 2 * * *", start_date=pendulum.datetime(2026, 11, 1, tz="UTC"), catchup=False,
     tags=["bronze", "F6"], default_args={"retries": 3, "retry_exponential_backoff": True})
def bronze_f6_events():
    @task(queue="bronze", outlets=[BRONZE_F6_EVENTS])
    def snapshot(data_interval_end=None) -> int:
        body, n = dump(SQL)
        put(f"bronze/f6_events/fecha={data_interval_end:%Y-%m-%d}.jsonl", body, "application/x-ndjson")
        return n

    snapshot()


bronze_f6_events()
