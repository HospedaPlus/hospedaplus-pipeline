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
         .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
         .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                 "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"))
    endpoint = os.getenv("S3_ENDPOINT_URL")
    if endpoint:
        b = b.config("spark.hadoop.fs.s3a.endpoint", endpoint).config("spark.hadoop.fs.s3a.path.style.access", "true")
    return b.getOrCreate()


def path(layer: str, name: str) -> str:
    return f"s3a://{os.getenv('S3_BUCKET', 'hospedaplus-lakehouse')}/{layer}/{name}"
