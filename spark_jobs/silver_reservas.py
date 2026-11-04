"""bronze (Neon reservations, every channel) -> silver/reservas, reservas_rechazadas, cancelaciones.

Every booking, whatever the channel (F2 OTA, F3 web/corporate, F4 agencies),
goes through operation.reservar(), so operation.reservations is the single
source for bookings. Bronze has it twice:
  f3_reservations_full/  one-off copy of the whole table (seed history)
  f3_reservations/       hourly files with what changed in each hour
The same code shows up several times (created, then cancelled, ...). Silver
keeps the newest version of each code.

Data quality defects (section 5.3) and what silver does with them:
  F3 zero guests / check-out before check-in -> Neon refuses them and the
      simulator logs them in reservation_anomalies -> silver/reservas_rechazadas
  F4 duplicated booking -> status 'duplicate' in Neon -> es_duplicado = true,
      left out of every metric
  F4 missing currency -> the loader used the hotel currency -> kept
  F2 events out of order -> already resolved in Neon (last_event_at)

Amounts go to COP (reporting currency) with silver/tasa_cambio_diaria on the
booking date. Bookings older than the first published rate use the oldest
rate available and are marked fx_estimada.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from spark_jobs import quality

LEAD_BUCKETS = [(1, "0-1 dias"), (3, "1-3 dias"), (7, "3-7 dias"), (30, "7-30 dias"), (90, "30-90 dias")]


def _ts(c: str):
    return F.to_timestamp(F.col(c))


def latest_version(raw: DataFrame) -> DataFrame:
    """One row per code: the newest state (greatest of created/cancelled/last event)."""
    version = F.greatest(_ts("created_at"), F.coalesce(_ts("cancelled_at"), _ts("created_at")),
                         F.coalesce(_ts("last_event_at"), _ts("created_at")))
    w = Window.partitionBy("code").orderBy(F.col("_version").desc(), F.col("_src").desc())
    return (raw.withColumn("_version", version)
            .withColumn("_rn", F.row_number().over(w))
            .filter("_rn = 1").drop("_rn", "_version", "_src"))


def lead_bucket(days):
    expr = F.when(days.isNull(), F.lit(None))
    for limit, label in LEAD_BUCKETS:
        expr = expr.when(days < limit, F.lit(label))
    return expr.otherwise(F.lit("90+ dias"))


def transform(raw: DataFrame, hotels: DataFrame, channels: DataFrame, room_types: DataFrame,
              fx: DataFrame) -> DataFrame:
    """raw: reservations as strings (bronze JSON) plus _src (0 = full copy, 1 = hourly)."""
    r = latest_version(raw)
    h = hotels.select(F.col("id").alias("hotel_id"), F.col("code").alias("hotel_code"),
                      F.col("country").alias("hotel_pais"), F.col("category").alias("hotel_categoria"),
                      F.col("timezone").alias("hotel_tz"))
    c = channels.select(F.col("id").alias("channel_id"), F.col("code").alias("channel_code"),
                        "channel_type", F.col("commission_pct").cast("double").alias("comision_pct"))
    t = room_types.select(F.col("id").alias("room_type_id"), F.col("code").alias("room_type"))

    df = (r.join(F.broadcast(h), "hotel_id", "left").join(F.broadcast(c), "channel_id", "left")
          .join(F.broadcast(t), "room_type_id", "left")
          .select(
              "code", "source", "external_code", "hotel_code", "hotel_pais", "hotel_categoria",
              "room_type", "channel_code", "channel_type", "comision_pct", "status",
              F.to_date("check_in").alias("check_in"), F.to_date("check_out").alias("check_out"),
              F.col("rooms_count").cast("int").alias("habitaciones"),
              F.col("adults").cast("int").alias("adultos"), F.col("children").cast("int").alias("ninos"),
              F.col("infants").cast("int").alias("infantes"),
              (F.lower(F.col("is_refundable")) == "true").alias("reembolsable"),
              "currency",
              F.col("first_night_amount").cast("double").alias("primera_noche"),
              F.col("total_amount").cast("double").alias("total"),
              F.col("penalty_amount").cast("double").alias("penalidad"),
              _ts("created_at").alias("creada_en"), _ts("cancelled_at").alias("cancelada_en"))
          .withColumn("fecha_reserva", F.to_date("creada_en"))
          .withColumn("noches", F.datediff("check_out", "check_in"))
          .withColumn("huespedes", F.col("adultos") + F.col("ninos"))
          .withColumn("lead_time_dias", F.datediff("check_in", "fecha_reserva"))
          .withColumn("es_duplicado", F.col("status") == "duplicate")
          .withColumn("es_cancelada", F.col("status").isin("cancelled", "no_show")))

    # FX: rate of the booking day; before the first published rate, the oldest one.
    first = fx.groupBy("moneda").agg(F.min_by("cop_por_unidad", "fecha").alias("_tasa_min"))
    df = (df.join(F.broadcast(fx.withColumnRenamed("moneda", "currency")
                              .withColumnRenamed("fecha", "fecha_reserva")
                              .withColumnRenamed("cop_por_unidad", "_tasa")),
                  ["currency", "fecha_reserva"], "left")
          .join(F.broadcast(first.withColumnRenamed("moneda", "currency")), "currency", "left")
          .withColumn("fx_estimada", F.col("_tasa").isNull())
          .withColumn("tasa_cop", F.coalesce("_tasa", "_tasa_min"))
          .drop("_tasa", "_tasa_min"))
    cop = F.col("tasa_cop")
    return (df.withColumn("total_cop", F.round(F.col("total") * cop, 2))
            .withColumn("penalidad_cop", F.round(F.col("penalidad") * cop, 2))
            .withColumn("comision_cop", F.round(F.col("total_cop") * F.col("comision_pct") / 100, 2))
            .withColumn("neto_cop", F.col("total_cop") - F.col("comision_cop")))


def cancelaciones(reservas: DataFrame) -> DataFrame:
    """Cancelled and no-show bookings with how long before check-in they happened (P1)."""
    horas = (F.unix_timestamp(F.to_timestamp(F.concat_ws(" ", F.col("check_in").cast("string"), F.lit("15:00:00"))))
             - F.unix_timestamp("cancelada_en")) / 3600
    return (reservas.filter("es_cancelada AND NOT es_duplicado")
            .select("code", "source", "hotel_code", "hotel_pais", "channel_code", "channel_type", "room_type",
                    "status", "reembolsable", "check_in", "fecha_reserva", "cancelada_en", "lead_time_dias",
                    "total_cop", "penalidad_cop")
            .withColumn("horas_antes_checkin", F.round(horas, 1))
            .withColumn("rango_anticipacion", lead_bucket(F.col("horas_antes_checkin") / 24)))


def rechazadas(raw_anomalies: DataFrame) -> DataFrame:
    return (raw_anomalies.dropDuplicates(["code"])
            .select("code", F.col("defect_type").alias("defecto"),
                    F.to_date("check_in").alias("check_in"), F.to_date("check_out").alias("check_out"),
                    F.col("adults").cast("int").alias("adultos"), F.col("children").cast("int").alias("ninos"),
                    "currency", F.col("total_amount").cast("double").alias("total"),
                    _ts("created_at").alias("registrada_en")))


def check(reservas: DataFrame, cancel: DataFrame) -> None:
    name = "silver.reservas"
    quality.not_empty(reservas, name)
    quality.unique(reservas, name, ["code"])
    sin_cop = (reservas.filter("total_cop IS NULL")
               .groupBy("source", "currency", F.col("total").isNull().alias("sin_total"),
                        F.col("tasa_cop").isNull().alias("sin_tasa"))
               .count().collect())
    if sin_cop:
        detalle = "; ".join(f"{r.source}/{r.currency} sin_total={r.sin_total} sin_tasa={r.sin_tasa}: {r['count']}"
                            for r in sin_cop)
        raise quality.QualityError(f"{name}: rows without total_cop -> {detalle}")
    quality.no_nulls(reservas, name, ["code", "source", "hotel_code", "channel_type", "check_in", "check_out",
                                      "status", "total_cop"])
    bad = reservas.filter("check_out <= check_in OR adultos < 1 OR total_cop < 0").count()
    if bad:
        raise quality.QualityError(f"{name}: {bad} rows break dates/guests/amount rules")
    quality.unique(cancel, "silver.cancelaciones", ["code"])


# ---- IO -----------------------------------------------------------------------------------------

def _read_json(s: SparkSession, paths: list[str]) -> DataFrame | None:
    found = []
    for p in paths:
        try:
            s.read.text(p).limit(1).collect()
            found.append(p)
        except Exception:  # path not there yet (e.g. full load never ran)
            pass
    if not found:
        return None
    return s.read.option("primitivesAsString", "true").json(found)


def _latest_dim(s: SparkSession, table: str) -> DataFrame:
    from spark_jobs.session import path
    df = s.read.option("primitivesAsString", "true").json(path("bronze", f"neon_dims/{table}/"))
    newest = df.select(F.max("fecha")).first()[0] if "fecha" in df.columns else None
    return df.filter(F.col("fecha") == newest) if newest else df


def _with_src(df: DataFrame | None, src: int) -> DataFrame | None:
    return None if df is None else df.withColumn("_src", F.lit(src))


def _union(a: DataFrame | None, b: DataFrame | None) -> DataFrame:
    parts = [d for d in (a, b) if d is not None]
    if not parts:
        raise quality.QualityError("bronze has no reservations yet")
    out = parts[0]
    for d in parts[1:]:
        out = out.unionByName(d, allowMissingColumns=True)
    return out


def run() -> dict[str, int]:
    from spark_jobs.session import path, spark
    s = spark("silver_reservas")
    raw = _union(_with_src(_read_json(s, [path("bronze", "f3_reservations_full/")]), 0),
                 _with_src(_read_json(s, [path("bronze", "f3_reservations/")]), 1))
    anomalies = _union(_read_json(s, [path("bronze", "f3_anomalies_full/")]),
                       _read_json(s, [path("bronze", "f3_anomalies/")]))
    fx = s.read.parquet(path("silver", "tasa_cambio_diaria"))
    reservas = transform(raw, _latest_dim(s, "hotels"), _latest_dim(s, "channels"),
                         _latest_dim(s, "room_types"), fx).cache()
    cancel = cancelaciones(reservas).cache()
    check(reservas, cancel)
    reservas.write.mode("overwrite").partitionBy("source").parquet(path("silver", "reservas"))
    cancel.write.mode("overwrite").parquet(path("silver", "cancelaciones"))
    rej = rechazadas(anomalies)
    rej.write.mode("overwrite").parquet(path("silver", "reservas_rechazadas"))
    return {"reservas": reservas.count(), "cancelaciones": cancel.count(), "rechazadas": rej.count()}
