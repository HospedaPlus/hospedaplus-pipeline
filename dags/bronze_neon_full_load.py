"""Nightly full snapshot of operation.reservations and reservation_anomalies -> S3 bronze.

bronze_neon_f2_f3 only brings what changes each hour. This snapshot (03:00 UTC)
brings the seed history from before the pipeline and squares silver with Neon
every night, whatever the hourly copy might have missed. Silver reads only the
newest snapshot plus the hourly files from the day before on. It reads by id in
pages so Neon and the worker never hold the whole table at once:
bronze/f3_reservations_full/fecha=YYYY-MM-DD/part-NNNNN.jsonl
bronze/f3_anomalies_full/fecha=YYYY-MM-DD/part-NNNNN.jsonl
Also the operational state silver needs for occupancy (P2/P3):
bronze/f3_inventory_full/      rooms total/sold/overbooking limit per hotel, room type and night
bronze/f3_relocations_full/    relocated bookings (with their hotel and room type)
bronze/f3_rejections_full/     bookings refused for lack of rooms
Silver merges this with the hourly files and keeps the newest version of each code.

Consistency and recovery:
- Every table is read in ONE read-only REPEATABLE READ transaction, so reservations,
  inventory and rejections describe the same instant in Neon.
- Parts left over from an earlier, larger copy of the same day are deleted.
- _SUCCESS is written in every folder only after all tables are copied. Silver reads
  only folders with _SUCCESS, so a copy that failed half way is never used; the retry
  (or tomorrow's run) overwrites it.
"""

from __future__ import annotations

import json

import pendulum
import psycopg2
from airflow.sdk import dag, task

from hospedaplus.lake import BRONZE_F3_FULL, BUCKET, neon_url, put, s3_client

PAGE = 50_000
TABLES = {  # Neon table -> bronze prefix
    "reservations": "f3_reservations_full",
    "reservation_anomalies": "f3_anomalies_full",
    "inventory": "f3_inventory_full",
    "reservation_rejections": "f3_rejections_full",
}
RELOCATIONS_PREFIX = "f3_relocations_full"

RELOCATIONS_SQL = """
SELECT l.*, r.code, r.hotel_id, r.room_type_id
  FROM operation.relocations l JOIN operation.reservations r ON r.id = l.reservation_id
"""


def _jsonl(cur) -> tuple[bytes, int]:
    cols = [c.name for c in cur.description]
    rows = cur.fetchall()
    lines = [json.dumps(dict(zip(cols, r)), default=str, ensure_ascii=False) for r in rows]
    return ("\n".join(lines) + ("\n" if lines else "")).encode(), len(rows)


def _folder(prefix: str, day: str) -> str:
    return f"bronze/{prefix}/fecha={day}/"


def _copy(cur, table: str, prefix: str, day: str) -> tuple[int, int]:
    """Page by id inside the open transaction. Returns (rows, parts written)."""
    last, part, total = 0, 0, 0
    while True:
        cur.execute(f"SELECT * FROM operation.{table} WHERE id > %s ORDER BY id LIMIT %s", (last, PAGE))
        body, n = _jsonl(cur)
        if n == 0:
            break
        put(f"{_folder(prefix, day)}part-{part:05d}.jsonl", body, "application/x-ndjson")
        last = json.loads(body.rstrip(b"\n").rsplit(b"\n", 1)[-1])["id"]
        part, total = part + 1, total + n
    if part == 0:  # empty table: one empty part so the folder exists
        put(f"{_folder(prefix, day)}part-00000.jsonl", b"", "application/x-ndjson")
        part = 1
    return total, part


def stale_keys(keys: list[str], parts: int) -> list[str]:
    """Keys of part files numbered >= parts (left by an earlier run of the same day), and an old _SUCCESS."""
    out = []
    for k in keys:
        name = k.rsplit("/", 1)[-1]
        if name == "_SUCCESS" or (name.startswith("part-") and int(name[5:10]) >= parts):
            out.append(k)
    return out


def _clean(prefix: str, day: str, parts: int) -> None:
    s3 = s3_client()
    res = s3.list_objects_v2(Bucket=BUCKET, Prefix=_folder(prefix, day))
    old = stale_keys([o["Key"] for o in res.get("Contents", [])], parts)
    if old:
        s3.delete_objects(Bucket=BUCKET, Delete={"Objects": [{"Key": k} for k in old]})


@dag(schedule="0 3 * * *", start_date=pendulum.datetime(2026, 11, 1, tz="UTC"), catchup=False,
     max_active_runs=1, tags=["bronze", "F2", "F3", "F4"], default_args={"retries": 2})
def bronze_neon_full_load():
    @task(queue="bronze", outlets=[BRONZE_F3_FULL])
    def copy_all() -> dict[str, int]:
        day = pendulum.now("UTC").to_date_string()  # the snapshot is of "now", whatever run it is
        counts, parts = {}, {}
        s3 = s3_client()
        for prefix in [RELOCATIONS_PREFIX, *TABLES.values()]:  # a re-run is unfinished until it ends
            s3.delete_object(Bucket=BUCKET, Key=f"{_folder(prefix, day)}_SUCCESS")
        with psycopg2.connect(neon_url()) as conn:
            conn.set_session(isolation_level="REPEATABLE READ", readonly=True)
            with conn.cursor() as cur:
                cur.execute(RELOCATIONS_SQL)  # small table, no id column of its own
                body, counts["relocations"] = _jsonl(cur)
                put(f"{_folder(RELOCATIONS_PREFIX, day)}part-00000.jsonl", body, "application/x-ndjson")
                parts[RELOCATIONS_PREFIX] = 1
                for table, prefix in TABLES.items():
                    counts[table], parts[prefix] = _copy(cur, table, prefix, day)
        for prefix, n in parts.items():  # only once everything is copied
            _clean(prefix, day, n)
            put(f"{_folder(prefix, day)}_SUCCESS", json.dumps(counts).encode())
        return counts

    copy_all()


bronze_neon_full_load()
