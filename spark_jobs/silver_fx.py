"""bronze/f7_fx/*.json -> silver/tasa_cambio_diaria (parquet).

Frankfurter v2 answers with rows (date, base, quote, rate), base USD.
cop_por_unidad(moneda) = rate(USD->COP) / rate(USD->moneda); USD = rate(USD->COP).
Weekends and holidays have no publication, so every calendar day takes the
last known rate (forward fill).
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from spark_jobs import quality
from spark_jobs.session import path, spark


def transform(raw: DataFrame) -> DataFrame:
    # v2 mixes providers: each quote can carry its own publication date (EUR/MXN
    # are not always dated like COP). So first fill every currency's USD rate on a
    # daily grid, and only then divide by the COP rate of the same day.
    rates = (raw.select(F.to_date("date").alias("fecha"), F.col("quote").alias("moneda"),
                        F.col("rate").cast("double").alias("rate"))
             .dropna().dropDuplicates(["fecha", "moneda"])
             .unionByName(raw.select(F.to_date("date").alias("fecha")).dropna().distinct()
                          .select("fecha", F.lit("USD").alias("moneda"), F.lit(1.0).alias("rate"))))
    bounds = rates.agg(F.min("fecha").alias("a"), F.max("fecha").alias("b"))
    days = bounds.select(F.explode(F.sequence("a", "b")).alias("fecha"))
    grid = days.crossJoin(rates.select("moneda").distinct())
    w = Window.partitionBy("moneda").orderBy("fecha").rowsBetween(Window.unboundedPreceding, 0)
    filled = (grid.join(rates, ["fecha", "moneda"], "left")
              .withColumn("rate", F.last("rate", ignorenulls=True).over(w))
              .dropna(subset=["rate"]))
    cop = filled.filter("moneda = 'COP'").select("fecha", F.col("rate").alias("usd_cop"))
    return (filled.join(cop, "fecha")
            .select("fecha", "moneda", (F.col("usd_cop") / F.col("rate")).alias("cop_por_unidad")))


def check(df: DataFrame) -> None:
    name = "silver.tasa_cambio_diaria"
    quality.not_empty(df, name)
    quality.no_nulls(df, name, ["fecha", "moneda", "cop_por_unidad"])
    quality.unique(df, name, ["fecha", "moneda"])
    quality.positive(df, name, "cop_por_unidad")
    quality.no_gaps(df, name, "fecha", "moneda")


def run() -> int:
    s = spark("silver_fx")
    raw = s.read.option("multiLine", "true").json(path("bronze", "f7_fx/"))
    filled = transform(raw)
    check(filled)
    filled.write.mode("overwrite").parquet(path("silver", "tasa_cambio_diaria"))
    return filled.count()
