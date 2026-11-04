"""bronze (Neon reservations, every channel) -> silver/reservas, reservas_rechazadas, cancelaciones.

Every booking, whatever the channel (F2 OTA, F3 web/corporate, F4 agencies),
goes through operation.reservar(), so operation.reservations is the single
source for bookings. Bronze has it twice:
  f3_reservations_full/  nightly copy of the whole table (only the newest is read)
  f3_reservations/       hourly files with what changed in each hour (read from
                         the day before the newest snapshot on)
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
from pyspark.sql import types as T

from spark_jobs import quality

# Bronze columns, all read as text (dump() writes NUMERIC/dates as strings).
# An explicit schema skips Spark's inference pass, which reads every file twice.
RES_COLS = ["id", "code", "source", "external_code", "hotel_id", "room_type_id", "channel_id", "guest_id",
            "check_in", "check_out", "rooms_count", "adults", "children", "infants", "status", "is_refundable",
            "currency", "first_night_amount", "total_amount", "penalty_amount", "duplicate_of",
            "created_at", "cancelled_at", "last_event_at", "updated_at"]
ANOM_COLS = ["id", "code", "hotel_id", "room_type_id", "channel_id", "guest_id", "check_in", "check_out",
             "adults", "children", "infants", "currency", "total_amount", "defect_type", "created_at"]


def _schema(cols: list[str]) -> T.StructType:
    return T.StructType([T.StructField(c, T.StringType()) for c in cols])


LEAD_BUCKETS = [(1, "0-1 dias"), (3, "1-3 dias"), (7, "3-7 dias"), (30, "7-30 dias"), (90, "30-90 dias")]


def _ts(c: str):
    return F.to_timestamp(F.col(c))


def latest_version(raw: DataFrame) -> DataFrame:
    """One row per code: the newest state.

    updated_at (Neon trigger, every change) when the row has it; older files
    without it fall back to the greatest of created/cancelled/last event.
    """
    legacy = F.greatest(_ts("created_at"), F.coalesce(_ts("cancelled_at"), _ts("created_at")),
                        F.coalesce(_ts("last_event_at"), _ts("created_at")))
    if "updated_at" not in raw.columns:
        raw = raw.withColumn("updated_at", F.lit(None).cast("string"))
    version = F.coalesce(_ts("updated_at"), legacy)
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

def _read_days(s: SparkSession, prefix: str, days: list[str], cols: list[str]) -> DataFrame | None:
    """Read only the fecha=... folders listed, with a fixed schema (no inference, no full listing)."""
    from spark_jobs.session import path
    if not days:
        return None
    base = path("bronze", f"{prefix}/")
    return (s.read.schema(_schema(cols)).option("basePath", base)
            .json([f"{base}fecha={d}/" for d in days]))


def latest_snapshot(df: DataFrame) -> DataFrame:
    """Keep only the newest daily snapshot of a catalog, one row per id.

    Files are named <table>/fecha=YYYY-MM-DD.jsonl (a file, not a folder), so Spark
    does not add a 'fecha' column: take it from the file name.
    """
    df = df.withColumn("_fecha", F.regexp_extract(F.input_file_name(), r"fecha=(\d{4}-\d{2}-\d{2})", 1))
    newest = df.agg(F.max("_fecha")).first()[0]
    return df.filter(F.col("_fecha") == newest).drop("_fecha").dropDuplicates(["id"])


def _latest_dim(s: SparkSession, table: str) -> DataFrame:
    from spark_jobs.session import path
    return latest_snapshot(s.read.option("primitivesAsString", "true").json(path("bronze", f"neon_dims/{table}/")))


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


def plan_days(full_days: list[str], hourly_days: list[str]) -> tuple[list[str], list[str]]:
    """Newest full snapshot + hourly days from the day before it on (dedup keeps the newest version)."""
    import datetime as dt
    if not full_days:
        return [], hourly_days
    newest = full_days[-1]
    since = (dt.date.fromisoformat(newest) - dt.timedelta(days=1)).isoformat()
    return [newest], [d for d in hourly_days if d >= since]


def run() -> dict[str, int]:
    from spark_jobs.session import partitions, path, spark
    s = spark("silver_reservas")
    full_d, hour_d = plan_days(partitions("bronze", "f3_reservations_full", complete=True),
                               partitions("bronze", "f3_reservations"))
    raw = _union(_with_src(_read_days(s, "f3_reservations_full", full_d, RES_COLS), 0),
                 _with_src(_read_days(s, "f3_reservations", hour_d, RES_COLS), 1))
    afull_d, ahour_d = plan_days(partitions("bronze", "f3_anomalies_full", complete=True),
                                 partitions("bronze", "f3_anomalies"))
    anomalies = _union(_read_days(s, "f3_anomalies_full", afull_d, ANOM_COLS),
                       _read_days(s, "f3_anomalies", ahour_d, ANOM_COLS))
    fx = s.read.parquet(path("silver", "tasa_cambio_diaria"))
    reservas = transform(raw, _latest_dim(s, "hotels"), _latest_dim(s, "channels"),
                         _latest_dim(s, "room_types"), fx).cache()
    cancel = cancelaciones(reservas).cache()
    rej = rechazadas(anomalies).cache()
    check(reservas, cancel)
    counts = {"reservas": reservas.count(), "cancelaciones": cancel.count(), "rechazadas": rej.count()}
    reservas.write.mode("overwrite").partitionBy("source").parquet(path("silver", "reservas"))
    cancel.write.mode("overwrite").parquet(path("silver", "cancelaciones"))
    rej.write.mode("overwrite").parquet(path("silver", "reservas_rechazadas"))
    return counts
