import datetime as dt
import json

import pytest
from pyspark.sql import SparkSession

from spark_jobs import quality, silver_fx


@pytest.fixture(scope="session")
def spark():
    s = SparkSession.builder.master("local[1]").appName("tests").config("spark.ui.enabled", "false").getOrCreate()
    yield s
    s.stop()


def rows(day, cop, eur):
    return [{"date": day, "base": "USD", "quote": "COP", "rate": cop},
            {"date": day, "base": "USD", "quote": "EUR", "rate": eur}]


def test_forward_fill_weekend(spark, tmp_path):
    # Friday and Monday published, weekend missing
    data = rows("2026-10-30", 4000.0, 0.8) + rows("2026-11-02", 4100.0, 0.82)
    f = tmp_path / "fx.json"
    f.write_text(json.dumps(data))
    raw = spark.read.option("multiLine", "true").json(str(f))
    out = silver_fx.transform(raw)
    silver_fx.check(out)
    got = {(r.fecha, r.moneda): r.cop_por_unidad for r in out.collect()}
    assert got[(dt.date(2026, 10, 31), "USD")] == 4000.0
    assert got[(dt.date(2026, 11, 1), "EUR")] == pytest.approx(5000.0)
    assert got[(dt.date(2026, 11, 2), "USD")] == 4100.0
    assert len(got) == 4 * 3  # 4 days x (COP, EUR, USD)


def test_duplicated_bronze_rows_are_dropped(spark, tmp_path):
    data = rows("2026-10-30", 4000.0, 0.8) * 2
    f = tmp_path / "fx.json"
    f.write_text(json.dumps(data))
    out = silver_fx.transform(spark.read.option("multiLine", "true").json(str(f)))
    silver_fx.check(out)
    assert out.count() == 3


def test_quality_rejects_bad_data(spark):
    df = spark.createDataFrame([(dt.date(2026, 11, 1), "USD", 0.0), (dt.date(2026, 11, 1), "USD", 1.0)],
                               "fecha date, moneda string, cop_por_unidad double")
    with pytest.raises(quality.QualityError):
        quality.unique(df, "t", ["fecha", "moneda"])
    with pytest.raises(quality.QualityError):
        quality.positive(df, "t", "cop_por_unidad")
    gap = spark.createDataFrame([(dt.date(2026, 11, 1), "USD", 1.0), (dt.date(2026, 11, 3), "USD", 1.0)],
                                "fecha date, moneda string, cop_por_unidad double")
    with pytest.raises(quality.QualityError):
        quality.no_gaps(gap, "t", "fecha", "moneda")
    with pytest.raises(quality.QualityError):
        quality.not_empty(df.limit(0), "t")


def test_quotes_with_different_dates(spark, tmp_path):
    # EUR published on Friday only, COP on Friday and Monday: EUR must still exist Monday
    data = rows("2026-10-30", 4000.0, 0.8) + [{"date": "2026-11-02", "base": "USD", "quote": "COP", "rate": 4100.0}]
    f = tmp_path / "fx.json"
    f.write_text(json.dumps(data))
    out = silver_fx.transform(spark.read.option("multiLine", "true").json(str(f)))
    silver_fx.check(out)
    got = {(r.fecha, r.moneda): r.cop_por_unidad for r in out.collect()}
    assert got[(dt.date(2026, 11, 2), "EUR")] == 4100.0 / 0.8
    assert got[(dt.date(2026, 11, 2), "USD")] == 4100.0
