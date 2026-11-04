"""bronze (nightly Neon snapshot) -> silver/ocupacion_diaria: one row per hotel, room type and night.

Inventory says how many rooms exist, how many are sold and how far the hotel may
overbook each night (P2/P3). Relocations and refused bookings are added per night,
so gold can show occupancy, RevPAR and how often overbooking hurt a guest.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from spark_jobs import quality
from spark_jobs import silver_reservas as sr

INV_COLS = ["id", "hotel_id", "room_type_id", "inventory_date", "total_rooms", "sold_rooms",
            "overbooking_limit", "available_rooms"]
REL_COLS = ["reservation_id", "relocation_date", "reason", "estimated_cost", "currency", "created_at",
            "code", "hotel_id", "room_type_id"]
REJ_COLS = ["id", "source", "channel_id", "external_code", "hotel_id", "room_type_id", "check_in", "check_out",
            "rooms_count", "reason", "nights_without_room", "rejected_at"]
KEY = ["hotel_id", "room_type_id", "fecha"]


def transform(inv: DataFrame, rel: DataFrame | None, rej: DataFrame | None, hotels: DataFrame,
              room_types: DataFrame) -> DataFrame:
    base = inv.select("hotel_id", "room_type_id", F.to_date("inventory_date").alias("fecha"),
                      F.col("total_rooms").cast("int").alias("habitaciones"),
                      F.col("sold_rooms").cast("int").alias("vendidas"),
                      F.col("overbooking_limit").cast("int").alias("limite_sobreventa"))
    if rel is not None:
        r = (rel.groupBy("hotel_id", "room_type_id", F.to_date("relocation_date").alias("fecha"))
             .agg(F.count("*").alias("reubicadas")))
        base = base.join(r, KEY, "left")
    else:
        base = base.withColumn("reubicadas", F.lit(None).cast("long"))
    if rej is not None:
        nights = F.explode(F.sequence(F.to_date("check_in"), F.date_sub(F.to_date("check_out"), 1)))
        j = (rej.select("hotel_id", "room_type_id", nights.alias("fecha"), F.col("rooms_count").cast("int").alias("n"))
             .groupBy(*KEY).agg(F.sum("n").alias("habitaciones_rechazadas")))
        base = base.join(j, KEY, "left")
    else:
        base = base.withColumn("habitaciones_rechazadas", F.lit(None).cast("long"))
    h = hotels.select(F.col("id").alias("hotel_id"), F.col("code").alias("hotel_code"),
                      F.col("country").alias("hotel_pais"))
    t = room_types.select(F.col("id").alias("room_type_id"), F.col("code").alias("room_type"))
    return (base.join(F.broadcast(h), "hotel_id", "left").join(F.broadcast(t), "room_type_id", "left")
            .fillna(0, ["reubicadas", "habitaciones_rechazadas"])
            .withColumn("disponibles", F.col("habitaciones") + F.col("limite_sobreventa") - F.col("vendidas"))
            .withColumn("ocupacion_pct", F.round(F.when(F.col("habitaciones") > 0,
                                                         F.col("vendidas") * 100.0 / F.col("habitaciones")), 2))
            .withColumn("sobrevendidas", F.greatest(F.lit(0), F.col("vendidas") - F.col("habitaciones")))
            .drop("hotel_id", "room_type_id"))


def check(df: DataFrame) -> None:
    name = "silver.ocupacion_diaria"
    quality.not_empty(df, name)
    quality.unique(df, name, ["hotel_code", "room_type", "fecha"])
    quality.no_nulls(df, name, ["hotel_code", "room_type", "fecha", "habitaciones", "vendidas"])
    bad = df.filter("vendidas > habitaciones + limite_sobreventa OR vendidas < 0").count()
    if bad:
        raise quality.QualityError(f"{name}: {bad} nights sold past capacity + overbooking limit")


def _snapshot(s, prefix: str, cols: list[str]):
    from spark_jobs.session import partitions
    days = partitions("bronze", prefix, complete=True)
    return sr._read_days(s, prefix, days[-1:], cols)


def run() -> int:
    from spark_jobs.session import path, spark
    s = spark("silver_ocupacion")
    inv = _snapshot(s, "f3_inventory_full", INV_COLS)
    if inv is None:
        raise quality.QualityError("bronze has no inventory snapshot yet (bronze_neon_full_load)")
    out = transform(inv, _snapshot(s, "f3_relocations_full", REL_COLS), _snapshot(s, "f3_rejections_full", REJ_COLS),
                    sr._latest_dim(s, "hotels"), sr._latest_dim(s, "room_types")).cache()
    check(out)
    n = out.count()
    out.write.mode("overwrite").parquet(path("silver", "ocupacion_diaria"))
    return n
