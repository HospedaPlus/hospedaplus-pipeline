"""F4 agency files -> bronze, weekly.

The files are generated on the sources EC2 (hospedaplus-csv, service f4-weekly), which
copies them unchanged to s3://<bucket>/bronze/f4_agencias/semana=<YYYY-Www>/ every Monday
06:00 UTC. This DAG runs after that, checks that the week landed complete (15 agency files
+ summary) and marks the asset, so silver starts by itself.
"""

from __future__ import annotations

import pendulum
from airflow.sdk import dag, task

from hospedaplus.lake import BRONZE_F4_AGENCIES, BUCKET, s3_client

EXPECTED_AGENCIES = 15


@dag(schedule="0 7 * * 1", start_date=pendulum.datetime(2026, 11, 1, tz="UTC"), catchup=False,
     tags=["bronze", "F4"], default_args={"retries": 6, "retry_delay": pendulum.duration(minutes=20)})
def bronze_f4_agencias():
    @task(queue="bronze", outlets=[BRONZE_F4_AGENCIES])
    def check_week(data_interval_end=None) -> int:
        week = data_interval_end.strftime("%G-W%V")
        prefix = f"bronze/f4_agencias/semana={week}/"
        resp = s3_client().list_objects_v2(Bucket=BUCKET, Prefix=prefix)
        names = [o["Key"].rsplit("/", 1)[-1] for o in resp.get("Contents", [])]
        agency_files = [n for n in names if n.startswith("AG")]
        if len(agency_files) < EXPECTED_AGENCIES or "_generation_summary.json" not in names:
            raise RuntimeError(f"{prefix}: {len(agency_files)}/{EXPECTED_AGENCIES} agency files yet; "
                               "is f4-weekly running with F4_S3_BUCKET set?")
        return len(agency_files)

    check_week()


bronze_f4_agencias()
