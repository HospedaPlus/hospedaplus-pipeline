"""bronze F6 (Nager holidays + team local events) -> silver/calendario_hotel.

One row per hotel and calendar day, with whether it is a public holiday in the
hotel's country and which local events are on. Gold joins bookings and occupancy
to it to measure the effect of holidays and events on demand and price (P3).
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from spark_jobs import quality
from spark_jobs import silver_reservas as sr

CATEGORY_RANK = {"low": 1, "medium": 2, "high": 3}


def holidays(raw: DataFrame) -> DataFrame:
    """(pais, fecha, nombre_festivo) from the Nager files, whatever their exact shape.

    v3 answers a list of {date, localName, name, ...}; if the payload wraps the list in an
    object, the first array of structs that has a 'date' is used.
    """
    df = raw.withColumn("_pais", F.regexp_extract(F.input_file_name(), r"pais=([A-Z]{2})", 1))
    if "date" not in df.columns:
        arr = [f.name for f in df.schema.fields if isinstance(f.dataType, T.ArrayType)
               and isinstance(f.dataType.elementType, T.StructType)
               and "date" in f.dataType.elementType.fieldNames()]
        if not arr:
            raise quality.QualityError("f6_holidays: no 'date' field in the Nager payload")
        df = df.select("_pais", F.explode(arr[0]).alias("h")).select("_pais", "h.*")
    name_cols = [c for c in ("localName", "name") if c in df.columns]
    nombre = F.coalesce(*[F.col(c) for c in name_cols]) if name_cols else F.lit(None).cast("string")
    return (df.select(F.col("_pais").alias("pais"), F.to_date("date").alias("fecha"), nombre.alias("nombre_festivo"))
            .dropna(subset=["pais", "fecha"])
            .groupBy("pais", "fecha").agg(F.concat_ws(" / ", F.collect_set("nombre_festivo")).alias("nombre_festivo")))


def transform(hol: DataFrame, events: DataFrame | None, hotels: DataFrame) -> DataFrame:
    h = hotels.select(F.col("code").alias("hotel_code"), F.col("country").alias("pais"))
    bounds = hol.agg(F.min("fecha").alias("a"), F.max("fecha").alias("b"))
    days = bounds.select(F.explode(F.sequence("a", "b")).alias("fecha"))
    cal = (h.crossJoin(days).join(hol, ["pais", "fecha"], "left")
           .withColumn("es_festivo", F.col("nombre_festivo").isNotNull()))
    if events is not None:
        rank = F.create_map(*[x for k, v in CATEGORY_RANK.items() for x in (F.lit(k), F.lit(v))])
        ev = (events.select("hotel_code", "name", "category",
                            F.explode(F.sequence(F.to_date("start_date"), F.to_date("end_date"))).alias("fecha"))
              .withColumn("_rank", rank[F.col("category")])
              .groupBy("hotel_code", "fecha")
              .agg(F.concat_ws(" / ", F.collect_set("name")).alias("evento"), F.max("_rank").alias("_rank")))
        inv = F.create_map(*[x for k, v in CATEGORY_RANK.items() for x in (F.lit(v), F.lit(k))])
        cal = (cal.join(ev, ["hotel_code", "fecha"], "left")
               .withColumn("categoria_evento", inv[F.col("_rank")]).drop("_rank"))
    else:
        cal = cal.withColumn("evento", F.lit(None).cast("string")).withColumn("categoria_evento",
                                                                              F.lit(None).cast("string"))
    dow = F.dayofweek("fecha")  # 1 = Sunday
    return (cal.withColumn("hay_evento", F.col("evento").isNotNull())
            .withColumn("anio", F.year("fecha")).withColumn("mes", F.month("fecha"))
            .withColumn("dia_semana", F.date_format("fecha", "EEEE"))
            .withColumn("es_fin_de_semana", dow.isin(1, 7)))


def check(df: DataFrame) -> None:
    name = "silver.calendario_hotel"
    quality.not_empty(df, name)
    quality.unique(df, name, ["hotel_code", "fecha"])
    quality.no_nulls(df, name, ["hotel_code", "pais", "fecha"])
    quality.no_gaps(df, name, "fecha", "hotel_code")


def run() -> int:
    from spark_jobs.session import path, spark
    s = spark("silver_calendario")
    hol = holidays(s.read.option("multiLine", "true").json(path("bronze", "f6_holidays/")))
    ev_days = [d.removesuffix(".jsonl") for d in _event_days()]
    events = None
    if ev_days:
        events = (s.read.option("primitivesAsString", "true")
                  .json(path("bronze", f"f6_events/fecha={ev_days[-1]}.jsonl")))
    out = transform(hol, events, sr._latest_dim(s, "hotels")).cache()
    check(out)
    n = out.count()
    out.write.mode("overwrite").parquet(path("silver", "calendario_hotel"))
    return n


def _event_days() -> list[str]:
    """Dates of the daily f6_events snapshots (files fecha=YYYY-MM-DD.jsonl), oldest first."""
    import os

    import boto3
    bucket = os.getenv("S3_BUCKET", "hospedaplus-lakehouse")
    s3 = boto3.client("s3", endpoint_url=os.getenv("S3_ENDPOINT_URL") or None)
    out = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix="bronze/f6_events/fecha="):
        for o in page.get("Contents", []):
            out.append(o["Key"].rsplit("fecha=", 1)[-1])
    return sorted(out)
