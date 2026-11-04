import datetime as dt
import json

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from spark_jobs import quality
from spark_jobs import silver_reservas as sr


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[1]").appName("tests").config("spark.ui.enabled", "false") \
        .config("spark.sql.session.timeZone", "UTC").getOrCreate()


def res(code, status="confirmed", created="2026-11-01T10:00:00", cancelled=None, last=None, src=1, cur="USD",
        source="F3", total="100.00", penalty="0", updated=None):
    return {"code": code, "source": source, "external_code": None, "hotel_id": "1", "channel_id": "1",
            "room_type_id": "1", "status": status, "check_in": "2026-11-10", "check_out": "2026-11-12",
            "rooms_count": "1", "adults": "2", "children": "1", "infants": "0", "is_refundable": "true",
            "currency": cur, "first_night_amount": "50", "total_amount": total, "penalty_amount": penalty,
            "created_at": created, "cancelled_at": cancelled, "last_event_at": last, "updated_at": updated,
            "_src": src}


def frame(spark, rows):
    # same as bronze: JSON lines read with every primitive as string
    rdd = spark.sparkContext.parallelize([json.dumps(r) for r in rows])
    df = spark.read.option("primitivesAsString", "true").json(rdd)
    for c in ("cancelled_at", "last_event_at", "external_code", "updated_at"):
        if c not in df.columns:
            df = df.withColumn(c, F.lit(None).cast("string"))
    return df.withColumn("_src", F.col("_src").cast("int"))


def dims(spark):
    hotels = spark.createDataFrame([{"id": "1", "code": "BV-BOG", "country": "CO", "category": "4", "timezone": "x"}])
    channels = spark.createDataFrame([{"id": "1", "code": "BOOKING", "channel_type": "OTA", "commission_pct": "15"}])
    rooms = spark.createDataFrame([{"id": "1", "code": "DBL"}])
    fx = spark.createDataFrame([(dt.date(2026, 11, 1), "USD", 4000.0), (dt.date(2026, 11, 2), "USD", 4100.0)],
                               "fecha date, moneda string, cop_por_unidad double")
    return hotels, channels, rooms, fx


def test_latest_version_cop_and_cancel(spark):
    raw = frame(spark, [
        res("A", src=0),
        res("A", status="cancelled", cancelled="2026-11-09T15:00:00", penalty="50"),
        res("B", created="2026-01-01T00:00:00"),
        res("C", status="duplicate", source="F4"),
    ])
    out = sr.transform(raw, *dims(spark))
    sr.check(out, sr.cancelaciones(out))
    got = {r.code: r for r in out.collect()}
    assert len(got) == 3
    a = got["A"]
    assert a.status == "cancelled" and a.total_cop == 400000.0 and a.comision_cop == 60000.0
    assert a.noches == 2 and a.huespedes == 3 and a.lead_time_dias == 9 and not a.fx_estimada
    assert got["B"].fx_estimada and got["B"].tasa_cop == 4000.0
    assert got["C"].es_duplicado
    c = sr.cancelaciones(out).collect()
    assert [r.code for r in c] == ["A"]
    assert c[0].horas_antes_checkin == 24.0 and c[0].rango_anticipacion == "1-3 dias"


def test_check_rejects_duplicate_codes(spark):
    raw = frame(spark, [res("A")])
    out = sr.transform(raw, *dims(spark))
    with pytest.raises(quality.QualityError):
        sr.check(out.unionByName(out), sr.cancelaciones(out))


def test_latest_snapshot_keeps_newest_file(spark, tmp_path):
    d = tmp_path / "hotels"
    d.mkdir()
    (d / "fecha=2026-11-03.jsonl").write_text('{"id": "1", "code": "OLD"}\n')
    (d / "fecha=2026-11-04.jsonl").write_text('{"id": "1", "code": "NEW"}\n')
    out = sr.latest_snapshot(spark.read.option("primitivesAsString", "true").json(str(d))).collect()
    assert [r.code for r in out] == ["NEW"]


def test_updated_at_wins_over_event_dates(spark):
    # no_show changes no date of its own: only updated_at says the full snapshot is newer
    raw = frame(spark, [
        res("A", src=1, updated="2026-11-02T10:00:00"),
        res("A", status="no_show", src=0, updated="2026-11-04T03:00:00"),
    ])
    out = sr.latest_version(raw).collect()
    assert [r.status for r in out] == ["no_show"]
