"""Neon reference tables -> S3 bronze, daily full snapshot.

Silver needs them to turn ids into catalog codes (hotel, channel, room type),
the channel commission (P4), the cancellation policy (P1) and the nightly rates (gold price, P3). They are small,
so each run writes the whole table:
bronze/neon_dims/<table>/fecha=YYYY-MM-DD.jsonl
Guests are left out on purpose (PII stays in Neon).
"""

from __future__ import annotations

import pendulum
from airflow.sdk import dag, task

from hospedaplus.lake import BRONZE_NEON_DIMS, put
from hospedaplus.neon import dump

TABLES = {
    "hotels": "SELECT id, code, name, category, city, country, timezone, currency, is_active FROM operation.hotels",
    "room_types": "SELECT id, code, name, max_guests FROM operation.room_types",
    "channels": "SELECT id, code, name, channel_type, commission_pct, source FROM operation.channels",
    "cancellation_policies": "SELECT * FROM operation.cancellation_policies",
    # Nightly rate per hotel, room type and season period: base of the suggested price (gold, RN-05/06).
    "rates": ("SELECT id, hotel_id, room_type_id, season_code, valid_from, valid_to, currency, amount"
              " FROM operation.rates"),
}


@dag(schedule="15 3 * * *", start_date=pendulum.datetime(2026, 11, 1, tz="UTC"), catchup=False,
     tags=["bronze", "F3"], default_args={"retries": 3, "retry_exponential_backoff": True})
def bronze_neon_dims():
    @task(queue="bronze", outlets=[BRONZE_NEON_DIMS])
    def snapshot(data_interval_end=None) -> dict[str, int]:
        counts = {}
        for table, sql in TABLES.items():
            body, counts[table] = dump(sql)
            put(f"bronze/neon_dims/{table}/fecha={data_interval_end:%Y-%m-%d}.jsonl", body, "application/x-ndjson")
        return counts

    snapshot()


bronze_neon_dims()
