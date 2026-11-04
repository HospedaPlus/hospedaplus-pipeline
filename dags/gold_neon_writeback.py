"""Gold -> Neon: operation.cancellation_rate_by_lead (P1), then, if enabled, the overbooking limits (P2).

gold_modelo_daily writes gold/neon_cancellation_rate_by_lead/ (one JSON file, one row per hotel
and lead bucket). This DAG upserts it into Neon with the app_writer role (NEON_WRITE_URL), so the
cancellation rates the business sees in Power BI are the same ones the operation uses.

With NEON_REFRESH_OVERBOOKING=true it then calls operation.refresh_overbooking_limits(), which
recomputes inventory.overbooking_limit from those rates. It is off by default because it changes
how many rooms the hotels may sell: turn it on on purpose.
Idempotent: the upsert replaces each (hotel_code, lead_bucket) row; re-running gives the same table.
"""

from __future__ import annotations

import json
import os

import pendulum
from airflow.sdk import dag, task

from hospedaplus.lake import BUCKET, GOLD_NEON_CANCEL_RATES, s3_client

PREFIX = "gold/neon_cancellation_rate_by_lead/"
UPSERT = """
INSERT INTO operation.cancellation_rate_by_lead (hotel_code, lead_bucket, cancel_rate, updated_at)
VALUES (%s, %s, %s, NOW())
ON CONFLICT (hotel_code, lead_bucket) DO UPDATE
   SET cancel_rate = EXCLUDED.cancel_rate, updated_at = EXCLUDED.updated_at
"""


def read_rates() -> list[tuple[str, str, float]]:
    s3 = s3_client()
    rows = []
    for obj in s3.list_objects_v2(Bucket=BUCKET, Prefix=PREFIX).get("Contents", []):
        if not obj["Key"].endswith(".json"):
            continue
        body = s3.get_object(Bucket=BUCKET, Key=obj["Key"])["Body"].read().decode()
        rows += [parse(line) for line in body.splitlines() if line.strip()]
    if not rows:
        raise RuntimeError(f"s3://{BUCKET}/{PREFIX} is empty: run gold_modelo_daily first")
    return rows


def parse(line: str) -> tuple[str, str, float]:
    r = json.loads(line)
    rate = float(r["cancel_rate"])
    if not 0 <= rate <= 1 or r["lead_bucket"] not in ("0-7", "8-30", "31-90", "91+"):
        raise ValueError(f"bad row for Neon: {r}")
    return r["hotel_code"], r["lead_bucket"], round(rate, 4)


@dag(schedule=[GOLD_NEON_CANCEL_RATES], start_date=pendulum.datetime(2026, 11, 1, tz="UTC"), catchup=False,
     max_active_runs=1, tags=["gold", "P1", "P2"], default_args={"retries": 3, "retry_exponential_backoff": True})
def gold_neon_writeback():
    @task(queue="gold")
    def upsert() -> dict:
        import psycopg2
        url = os.getenv("NEON_WRITE_URL", "")
        if not url:
            raise RuntimeError("NEON_WRITE_URL is empty: set it in the workers .env (role app_writer)")
        rows = read_rates()
        out = {"rates": len(rows), "limits_changed": None}
        with psycopg2.connect(url) as conn, conn.cursor() as cur:  # one transaction: all rows or none
            cur.executemany(UPSERT, rows)
            if os.getenv("NEON_REFRESH_OVERBOOKING", "false").lower() == "true":
                cur.execute("SELECT operation.refresh_overbooking_limits()")
                out["limits_changed"] = cur.fetchone()[0]
        return out

    upsert()


gold_neon_writeback()
