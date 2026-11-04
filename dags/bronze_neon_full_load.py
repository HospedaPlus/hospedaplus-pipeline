"""Nightly full snapshot of operation.reservations and reservation_anomalies -> S3 bronze.

bronze_neon_f2_f3 only brings what changes each hour. This snapshot (03:00 UTC)
brings the seed history from before the pipeline and squares silver with Neon
every night, whatever the hourly copy might have missed. Silver reads only the
newest snapshot plus the hourly files from the day before on. It reads by id in
pages so Neon and the worker never hold the whole table at once:
bronze/f3_reservations_full/fecha=YYYY-MM-DD/part-NNNNN.jsonl
bronze/f3_anomalies_full/fecha=YYYY-MM-DD/part-NNNNN.jsonl
Silver merges this with the hourly files and keeps the newest version of each code.
"""

from __future__ import annotations

import json

import pendulum
from airflow.sdk import dag, task

from hospedaplus.lake import BRONZE_F3_FULL, put
from hospedaplus.neon import dump

PAGE = 50_000


def _copy(table: str, prefix: str, day: str) -> int:
    last, part, total = 0, 0, 0
    while True:
        body, n = dump(f"SELECT * FROM operation.{table} WHERE id > %s ORDER BY id LIMIT %s", (last, PAGE))
        if n == 0:
            break
        put(f"bronze/{prefix}/fecha={day}/part-{part:05d}.jsonl", body, "application/x-ndjson")
        last = json.loads(body.rstrip(b"\n").rsplit(b"\n", 1)[-1])["id"]
        part, total = part + 1, total + n
    return total


@dag(schedule="0 3 * * *", start_date=pendulum.datetime(2026, 11, 1, tz="UTC"), catchup=False,
     tags=["bronze", "F2", "F3", "F4"], default_args={"retries": 2})
def bronze_neon_full_load():
    @task(queue="bronze", outlets=[BRONZE_F3_FULL])
    def copy_all() -> dict[str, int]:
        day = pendulum.now("UTC").to_date_string()
        return {"reservations": _copy("reservations", "f3_reservations_full", day),
                "anomalies": _copy("reservation_anomalies", "f3_anomalies_full", day)}

    copy_all()


bronze_neon_full_load()
