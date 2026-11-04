"""bronze F1 (Kaggle hotel_bookings.csv, 2015-2017, Portugal) -> silver/historico_f1.

The history behind P1: how cancellation depends on lead time, deposit type and
channel. Amounts stay in EUR (the rates in F7 start in 2026). Rows with no guests,
no nights or a negative ADR are dropped and counted.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from spark_jobs import quality

# market_segment -> our channel types (cancellation_policies.channel_type)
CHANNEL_TYPE = {"Online TA": "ota", "Offline TA/TO": "agency", "Groups": "agency", "Corporate": "corporate",
                "Direct": "direct", "Complementary": "direct", "Aviation": "corporate"}


def _int(c: str):
    return F.when(F.col(c).isin("NA", "NULL", ""), None).otherwise(F.col(c)).cast("int")


def transform(raw: DataFrame) -> DataFrame:
    seg = F.create_map(*[x for k, v in CHANNEL_TYPE.items() for x in (F.lit(k), F.lit(v))])
    arrival = F.to_date(F.concat_ws("-", "arrival_date_year", "arrival_date_month", "arrival_date_day_of_month"),
                        "yyyy-MMMM-d")
    df = raw.select(
        F.col("hotel").alias("hotel_tipo"),
        (F.col("is_canceled") == "1").alias("es_cancelada"),
        _int("lead_time").alias("lead_time_dias"),
        arrival.alias("fecha_llegada"),
        (_int("stays_in_weekend_nights") + _int("stays_in_week_nights")).alias("noches"),
        _int("adults").alias("adultos"), _int("children").alias("ninos"), _int("babies").alias("bebes"),
        F.col("meal").alias("regimen"), F.col("country").alias("pais_huesped"),
        F.col("market_segment").alias("segmento"), F.col("distribution_channel").alias("canal_distribucion"),
        F.coalesce(seg[F.col("market_segment")], F.lit("otro")).alias("channel_type"),
        (F.col("is_repeated_guest") == "1").alias("es_repetido"),
        _int("previous_cancellations").alias("cancelaciones_previas"),
        F.col("deposit_type").alias("deposit_type"), F.col("customer_type").alias("tipo_cliente"),
        F.col("adr").cast("double").alias("adr_eur"),
        F.col("reservation_status").alias("estado"), F.to_date("reservation_status_date").alias("fecha_estado"))
    return (df.withColumn("huespedes", F.coalesce("adultos", F.lit(0)) + F.coalesce("ninos", F.lit(0))
                          + F.coalesce("bebes", F.lit(0)))
            .withColumn("ingreso_eur", F.round(F.col("adr_eur") * F.col("noches"), 2))
            .withColumn("rango_anticipacion", _lead_bucket(F.col("lead_time_dias"))))


def _lead_bucket(days):
    from spark_jobs.silver_reservas import lead_bucket
    return lead_bucket(days)


def split_valid(df: DataFrame) -> tuple[DataFrame, int]:
    ok = "huespedes > 0 AND noches > 0 AND adr_eur >= 0 AND fecha_llegada IS NOT NULL"
    return df.filter(ok), df.filter(f"NOT ({ok})").count()


def check(df: DataFrame) -> None:
    name = "silver.historico_f1"
    quality.not_empty(df, name)
    quality.no_nulls(df, name, ["hotel_tipo", "es_cancelada", "lead_time_dias", "fecha_llegada", "deposit_type"])


def run() -> dict[str, int]:
    from spark_jobs.session import path, spark
    s = spark("silver_f1")
    raw = s.read.option("header", "true").csv(path("bronze", "f1_historical/hotel_bookings.csv"))
    valid, dropped = split_valid(transform(raw))
    valid = valid.cache()
    check(valid)
    n = valid.count()
    valid.write.mode("overwrite").parquet(path("silver", "historico_f1"))
    return {"filas": n, "descartadas": dropped}
