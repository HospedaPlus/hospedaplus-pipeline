"""F1 Kaggle hotel_bookings.csv -> S3 bronze/f1_historical/hotel_bookings.csv, once.

The file never changes. It is downloaded from the hospedaplus-csv repo (public) and
uploaded as is; the sha256 goes in the object metadata, and if the object already has
the same hash nothing is uploaded again. Run it by hand (schedule=None).
"""

from __future__ import annotations

import hashlib
import os

import pendulum
import requests
from airflow.sdk import dag, task

from hospedaplus.lake import BRONZE_F1_HISTORICAL, BUCKET, s3_client

F1_URL = os.getenv(
    "F1_CSV_URL",
    "https://raw.githubusercontent.com/HospedaPlus/hospedaplus-csv/main/data/f1_historical/hotel_bookings.csv",
)
KEY = "bronze/f1_historical/hotel_bookings.csv"


@dag(schedule=None, start_date=pendulum.datetime(2026, 11, 1, tz="UTC"), catchup=False,
     tags=["bronze", "F1"], default_args={"retries": 2})
def bronze_f1_historical():
    @task(queue="bronze", outlets=[BRONZE_F1_HISTORICAL])
    def upload() -> str:
        r = requests.get(F1_URL, timeout=120)
        r.raise_for_status()
        sha = hashlib.sha256(r.content).hexdigest()
        s3 = s3_client()
        try:
            if s3.head_object(Bucket=BUCKET, Key=KEY)["Metadata"].get("sha256") == sha:
                return f"already in s3://{BUCKET}/{KEY} (sha256 {sha[:12]})"
        except s3.exceptions.ClientError:
            pass
        s3.put_object(Bucket=BUCKET, Key=KEY, Body=r.content, ContentType="text/csv", Metadata={"sha256": sha})
        return f"uploaded {len(r.content):,} bytes to s3://{BUCKET}/{KEY}"

    upload()


bronze_f1_historical()
