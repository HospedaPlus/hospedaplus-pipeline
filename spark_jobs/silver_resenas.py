"""bronze F5 (hourly reviews API pages) -> silver/resenas (P5).

1. Latest version of each review id (a review edited later shows up again).
2. Cross-platform duplicates (defect 5.3: the same guest posts the same review on
   another platform, minutes or hours apart, with a name variant and maybe without
   the final period): same hotel, stay date, rating and text once normalized ->
   keep the first one published, and count on which other platforms it was copied.
3. Hotel country from the catalog and a sentiment band from the rating.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from spark_jobs import quality
from spark_jobs import silver_reservas as sr

REV_COLS = ["id", "external_id", "platform", "hotel_id", "language", "rating", "title", "text", "reviewer_name",
            "reviewer_country", "stay_date", "published_at", "created_at", "updated_at"]


def _norm_text(c):
    return F.regexp_replace(F.lower(F.trim(c)), r"[\s.!]+$", "")


def transform(raw: DataFrame, hotels: DataFrame) -> DataFrame:
    w_id = Window.partitionBy("id").orderBy(F.to_timestamp("updated_at").desc())
    latest = raw.withColumn("_rn", F.row_number().over(w_id)).filter("_rn = 1").drop("_rn")
    r = latest.select(
        F.col("id").cast("long").alias("review_id"), "external_id", F.col("platform").alias("plataforma"),
        F.col("hotel_id").alias("hotel_code"), F.col("language").alias("idioma"),
        F.col("rating").cast("int").alias("puntaje"), F.col("title").alias("titulo"), F.col("text").alias("texto"),
        F.col("reviewer_country").alias("pais_huesped"), F.to_date("stay_date").alias("fecha_estadia"),
        F.to_timestamp("published_at").alias("publicada_en"))
    key = ["hotel_code", "fecha_estadia", "puntaje", "_texto_norm"]
    w_dup = Window.partitionBy(*key).orderBy("publicada_en", "review_id")
    r = (r.withColumn("_texto_norm", _norm_text(F.col("texto")))
         .withColumn("_rn", F.row_number().over(w_dup))
         .withColumn("copias_en", F.collect_set("plataforma").over(Window.partitionBy(*key))))
    original = (r.filter("_rn = 1")
                .withColumn("copias_en", F.expr("array_remove(copias_en, plataforma)"))
                .withColumn("duplicados", F.size("copias_en"))
                .drop("_rn", "_texto_norm"))
    h = hotels.select(F.col("code").alias("hotel_code"), F.col("country").alias("hotel_pais"))
    return (original.join(F.broadcast(h), "hotel_code", "left")
            .withColumn("fecha_publicacion", F.to_date("publicada_en"))
            .withColumn("sentimiento", F.when(F.col("puntaje") >= 4, "positivo")
                        .when(F.col("puntaje") == 3, "neutro").otherwise("negativo")))


def check(df: DataFrame) -> None:
    name = "silver.resenas"
    quality.not_empty(df, name)
    quality.unique(df, name, ["review_id"])
    quality.no_nulls(df, name, ["review_id", "hotel_code", "plataforma", "puntaje", "publicada_en"])
    bad = df.filter("puntaje < 1 OR puntaje > 5 OR hotel_pais IS NULL").count()
    if bad:
        raise quality.QualityError(f"{name}: {bad} reviews with rating out of 1-5 or unknown hotel")


def run() -> dict[str, int]:
    from spark_jobs.session import partitions, path, spark
    s = spark("silver_resenas")
    raw = sr._read_days(s, "f5_reviews", partitions("bronze", "f5_reviews"), REV_COLS)
    if raw is None:
        raise quality.QualityError("bronze has no reviews yet")
    out = transform(raw, sr._latest_dim(s, "hotels")).cache()
    check(out)
    counts = {"resenas": out.count(), "duplicados_quitados": out.agg(F.sum("duplicados")).first()[0] or 0}
    out.write.mode("overwrite").partitionBy("fecha_publicacion").parquet(path("silver", "resenas"))
    return counts
