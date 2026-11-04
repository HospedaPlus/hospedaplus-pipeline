"""F5 reviews API -> S3 bronze/f5_reviews/fecha=YYYY-MM-DD/hora=HH.json, hourly, incremental.

GET {F5_API_URL}/api/v1/reviews?updated_from=<interval start>&page=N walks every page,
so edited (PATCHed) reviews are picked up too. A review edited during the run can
appear in two hours; silver keeps the latest version per review id. Duplicates between platforms are kept
here on purpose: silver removes them.
"""

from __future__ import annotations

import json
import os

import pendulum
import requests
from airflow.sdk import dag, task

from hospedaplus.lake import BRONZE_F5_REVIEWS, put

PAGE_SIZE = 100


@dag(schedule="@hourly", start_date=pendulum.datetime(2026, 11, 2, tz="UTC"), catchup=False,
     tags=["bronze", "F5"], default_args={"retries": 3, "retry_exponential_backoff": True})
def bronze_f5_reviews():
    @task(queue="bronze", outlets=[BRONZE_F5_REVIEWS])
    def extract(data_interval_start=None, data_interval_end=None) -> int:
        base = os.getenv("F5_API_URL", "").rstrip("/")
        if not base:
            raise RuntimeError("F5_API_URL is empty: set the reviews API URL in .env")
        headers = {"x-api-key": os.environ["F5_API_KEY"]} if os.getenv("F5_API_KEY") else {}
        items, page = [], 1
        while True:
            r = requests.get(f"{base}/api/v1/reviews", headers=headers, timeout=60, params={
                "updated_from": data_interval_start.isoformat(), "page": page, "page_size": PAGE_SIZE})
            r.raise_for_status()
            body = r.json()
            items += body["items"]
            if page >= body.get("total_pages", 1):
                break
            page += 1
        # The API only filters "from"; keep [start, end) so consecutive hours do not overlap.
        items = [i for i in items if i.get("updated_at") and pendulum.parse(i["updated_at"]) < data_interval_end]
        lines = "\n".join(json.dumps(i, ensure_ascii=False) for i in items)
        put(f"bronze/f5_reviews/fecha={data_interval_start:%Y-%m-%d}/hora={data_interval_start:%H}.jsonl",
            (lines + ("\n" if lines else "")).encode(), "application/x-ndjson")
        return len(items)

    extract()


bronze_f5_reviews()
