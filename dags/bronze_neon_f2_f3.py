"""F2 (ledger) and F3 (reservations) from Neon -> S3 bronze, every hour, incremental.

Each run copies only the rows that changed inside its data interval, as
JSON Lines, under a key named after the interval, so a re-run overwrites
the same file. Read with the etl_reader role.
"""

from __future__ import annotations

import json

import pendulum
import psycopg2
from airflow.sdk import dag, task

from hospedaplus.lake import BRONZE_F2_LEDGER, BRONZE_F3_RESERVATIONS, neon_url, put

F2_SQL = """
SELECT * FROM streaming.reservation_event_ledger
 WHERE received_at >= %s AND received_at < %s
"""
F3_SQL = """
SELECT * FROM operation.reservations
 WHERE greatest(created_at, coalesce(cancelled_at, created_at), coalesce(last_event_at, created_at)) >= %s
   AND greatest(created_at, coalesce(cancelled_at, created_at), coalesce(last_event_at, created_at)) < %s
"""
F3_ANOMALIES_SQL = """
SELECT * FROM operation.reservation_anomalies WHERE created_at >= %s AND created_at < %s
"""


def _dump(sql: str, start, end) -> tuple[bytes, int]:
    with psycopg2.connect(neon_url()) as conn, conn.cursor() as cur:
        cur.execute(sql, (start, end))
        cols = [c.name for c in cur.description]
        rows = cur.fetchall()
    lines = [json.dumps(dict(zip(cols, r)), default=str, ensure_ascii=False) for r in rows]
    return ("\n".join(lines) + ("\n" if lines else "")).encode(), len(rows)


def _key(prefix: str, start) -> str:
    return f"bronze/{prefix}/fecha={start:%Y-%m-%d}/hora={start:%H}.jsonl"


@dag(schedule="@hourly", start_date=pendulum.datetime(2026, 11, 1, tz="UTC"), catchup=False,
     tags=["bronze", "F2", "F3"], default_args={"retries": 3, "retry_exponential_backoff": True})
def bronze_neon_f2_f3():
    @task(queue="bronze", outlets=[BRONZE_F2_LEDGER])
    def f2_ledger(data_interval_start=None, data_interval_end=None) -> int:
        body, n = _dump(F2_SQL, data_interval_start, data_interval_end)
        put(_key("f2_ledger", data_interval_start), body, "application/x-ndjson")
        return n

    @task(queue="bronze", outlets=[BRONZE_F3_RESERVATIONS])
    def f3_reservations(data_interval_start=None, data_interval_end=None) -> int:
        body, n = _dump(F3_SQL, data_interval_start, data_interval_end)
        put(_key("f3_reservations", data_interval_start), body, "application/x-ndjson")
        body, m = _dump(F3_ANOMALIES_SQL, data_interval_start, data_interval_end)
        put(_key("f3_anomalies", data_interval_start), body, "application/x-ndjson")
        return n + m

    f2_ledger()
    f3_reservations()


bronze_neon_f2_f3()
