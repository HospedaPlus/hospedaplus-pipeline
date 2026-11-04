"""SparkSession in local mode with s3a access, used by every job."""

from __future__ import annotations

import glob
import os

from pyspark.sql import SparkSession


def spark(app: str) -> SparkSession:
    jars = ",".join(glob.glob("/opt/spark-jars/*.jar"))
    b = (SparkSession.builder.appName(app)
         .master(os.getenv("SPARK_MASTER", "local[*]"))
         .config("spark.jars", jars)
         .config("spark.sql.session.timeZone", "UTC")
         # Local mode: the driver is the executor. The default 1g spills to disk.
         .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "4g"))
         # ~200k rows: 200 shuffle tasks (the default) is pure overhead.
         .config("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "8"))
         .config("spark.sql.adaptive.enabled", "true")
         .config("spark.ui.enabled", "false")
         .config("spark.hadoop.fs.s3a.fast.upload", "true")
         .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
         .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                 "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"))
    endpoint = os.getenv("S3_ENDPOINT_URL")
    if endpoint:
        b = b.config("spark.hadoop.fs.s3a.endpoint", endpoint).config("spark.hadoop.fs.s3a.path.style.access", "true")
    return b.getOrCreate()


def path(layer: str, name: str) -> str:
    return f"s3a://{os.getenv('S3_BUCKET', 'hospedaplus-lakehouse')}/{layer}/{name}"


def complete_days(keys: list[str]) -> list[str]:
    """fecha=... folders that hold a finished snapshot, from a flat list of S3 keys.

    bronze_neon_full_load writes _SUCCESS last, so a folder without it is a copy that
    failed half way. Folders from before the marker existed count only while no folder
    has one yet, so the switch-over day keeps working.
    """
    days, done = set(), set()
    for k in keys:
        parts = k.split("/")
        folder = next((x for x in parts if x.startswith("fecha=")), None)
        if folder is None:
            continue
        day = folder.removeprefix("fecha=")
        days.add(day)
        if parts[-1] == "_SUCCESS":
            done.add(day)
    return sorted(done or days)


def partitions(layer: str, prefix: str, complete: bool = False) -> list[str]:
    """Names of the fecha=YYYY-MM-DD folders under layer/prefix, sorted (S3 listing, no Spark job).

    complete=True keeps only finished full snapshots (see complete_days)."""
    import boto3
    bucket = os.getenv("S3_BUCKET", "hospedaplus-lakehouse")
    s3 = boto3.client("s3", endpoint_url=os.getenv("S3_ENDPOINT_URL") or None)
    pages = s3.get_paginator("list_objects_v2")
    if complete:
        keys = [o["Key"] for page in pages.paginate(Bucket=bucket, Prefix=f"{layer}/{prefix}/")
                for o in page.get("Contents", [])]
        return complete_days(keys)
    out = []
    for page in pages.paginate(Bucket=bucket, Prefix=f"{layer}/{prefix}/", Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            name = cp["Prefix"].rstrip("/").rsplit("/", 1)[-1]
            if name.startswith("fecha="):
                out.append(name.removeprefix("fecha="))
    return sorted(out)
