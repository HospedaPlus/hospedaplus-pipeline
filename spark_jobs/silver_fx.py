"""bronze/f7_fx/*.json -> silver/tasa_cambio_diaria (parquet).

Frankfurter v2 answers with rows (date, base, quote, rate), base USD.
cop_por_unidad(moneda) = rate(USD->COP) / rate(USD->moneda); USD = rate(USD->COP).
Weekends and holidays have no publication, so every calendar day takes the
last known rate (forward fill).
"""

from __future__ import annotations

from pyspark.sql import Window
from pyspark.sql import functions as F

from spark_jobs.session import path, spark


def run() -> int:
    s = spark("silver_fx")
    raw = s.read.option("multiLine", "true").json(path("bronze", "f7_fx/"))
    rates = raw.select(F.to_date("date").alias("fecha_tasa"), F.col("quote").alias("moneda"),
                       F.col("rate").cast("double").alias("rate")).dropDuplicates(["fecha_tasa", "moneda"])
    cop = rates.filter("moneda = 'COP'").select("fecha_tasa", F.col("rate").alias("usd_cop"))
    per_ccy = (rates.join(cop, "fecha_tasa")
               .select("fecha_tasa", "moneda", (F.col("usd_cop") / F.col("rate")).alias("cop_por_unidad")))
    usd = cop.select("fecha_tasa", F.lit("USD").alias("moneda"), F.col("usd_cop").alias("cop_por_unidad"))
    per_ccy = per_ccy.unionByName(usd)

    bounds = per_ccy.agg(F.min("fecha_tasa").alias("a"), F.max("fecha_tasa").alias("b"))
    days = bounds.select(F.explode(F.sequence("a", "b")).alias("fecha"))
    grid = days.crossJoin(per_ccy.select("moneda").distinct())
    w = Window.partitionBy("moneda").orderBy("fecha").rowsBetween(Window.unboundedPreceding, 0)
    filled = (grid.join(per_ccy.withColumnRenamed("fecha_tasa", "fecha"), ["fecha", "moneda"], "left")
              .withColumn("cop_por_unidad", F.last("cop_por_unidad", ignorenulls=True).over(w))
              .dropna(subset=["cop_por_unidad"]))
    filled.write.mode("overwrite").parquet(path("silver", "tasa_cambio_diaria"))
    return filled.count()
