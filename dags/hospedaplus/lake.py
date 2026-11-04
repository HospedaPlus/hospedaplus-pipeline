"""Shared helpers for every DAG: S3 keys, assets and the Neon connection.

Bronze keeps each source exactly as it arrived. Keys depend only on the
source and the date, so re-running a task overwrites the same object
instead of duplicating it (idempotent).
"""

from __future__ import annotations

import os

import boto3
from airflow.sdk import Asset

BUCKET = os.getenv("S3_BUCKET", "hospedaplus-lakehouse")


def s3_client():
    return boto3.client("s3", endpoint_url=os.getenv("S3_ENDPOINT_URL") or None)


def put(key: str, body: bytes, content_type: str = "application/json") -> str:
    s3_client().put_object(Bucket=BUCKET, Key=key, Body=body, ContentType=content_type)
    return f"s3://{BUCKET}/{key}"


def neon_url() -> str:
    url = os.getenv("NEON_READ_URL", "")
    if not url:
        raise RuntimeError("NEON_READ_URL is empty: set it in .env (role etl_reader)")
    return url


def asset(layer: str, name: str) -> Asset:
    """One asset per dataset; a DAG scheduled on it runs when it is updated."""
    return Asset(f"s3://{BUCKET}/{layer}/{name}")


# Bronze
BRONZE_F1_HISTORICAL = asset("bronze", "f1_historical")
BRONZE_F4_AGENCIES = asset("bronze", "f4_agencias")
BRONZE_F5_REVIEWS = asset("bronze", "f5_reviews")
BRONZE_F6_EVENTS = asset("bronze", "f6_events")
BRONZE_F2_LEDGER = asset("bronze", "f2_ledger")
BRONZE_F3_RESERVATIONS = asset("bronze", "f3_reservations")
BRONZE_F3_ANOMALIES = asset("bronze", "f3_anomalies")
BRONZE_NEON_DIMS = asset("bronze", "neon_dims")
BRONZE_F6_HOLIDAYS = asset("bronze", "f6_holidays")
BRONZE_F7_FX = asset("bronze", "f7_fx")
# Silver
SILVER_FX_DAILY = asset("silver", "tasa_cambio_diaria")
SILVER_RESERVAS = asset("silver", "reservas")
SILVER_CANCELACIONES = asset("silver", "cancelaciones")
