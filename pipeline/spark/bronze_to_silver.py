"""Silver: bronze CSV pages → typed, deduplicated, quality-checked Parquet (partitioned by trip date).

Rows that break a rule are not silently dropped: they go to silver/taxi_trips_rejected with every reason, and a JSON
quality report counts them. The job fails when the reject rate is above MAX_REJECT_RATE (a broken source, not noise).
Run: spark-submit pipeline/spark/bronze_to_silver.py
"""
from __future__ import annotations

import json
import logging
import sys
from datetime import date, datetime, timezone

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from pipeline import storage
from pipeline.config import BRONZE_TRIPS, SILVER_QUALITY, SILVER_REJECTED, SILVER_TRIPS, Settings, s3a
from pipeline.spark.session import spark_session

log = logging.getLogger(__name__)

# The source's columns (SODA field names), all read as text: typing is this job's work, not the reader's guess.
RAW_COLUMNS = [
    "trip_id", "taxi_id", "trip_start_timestamp", "trip_end_timestamp", "trip_seconds", "trip_miles",
    "pickup_census_tract", "dropoff_census_tract", "pickup_community_area", "dropoff_community_area",
    "fare", "tips", "tolls", "extras", "trip_total", "payment_type", "company",
    "pickup_centroid_latitude", "pickup_centroid_longitude", "pickup_centroid_location",
    "dropoff_centroid_latitude", "dropoff_centroid_longitude", "dropoff_centroid_location",
]
RAW_SCHEMA = T.StructType([T.StructField(c, T.StringType()) for c in RAW_COLUMNS])
TS_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS"

# Bounds of a plausible trip (rejected outside).
MAX_SECONDS = 24 * 3600
MAX_MILES = 500.0
MAX_TOTAL = 1000.0


def read_bronze(spark: SparkSession, path: str) -> DataFrame:
    """Every CSV page under bronze (manifests excluded); `month` comes from the partition folder."""
    return (
        spark.read.option("header", "true").option("mode", "PERMISSIVE").option("pathGlobFilter", "*.csv")
        .schema(RAW_SCHEMA).csv(path)
        .withColumn("source_file", F.input_file_name())
    )


def typed(df: DataFrame) -> DataFrame:
    """Text → types. A value that cannot be parsed becomes null (and is caught by the rules below)."""
    num = lambda c: F.col(c).cast("double")  # noqa: E731
    return df.select(
        F.trim("trip_id").alias("trip_id"),
        F.trim("taxi_id").alias("taxi_id"),
        F.to_timestamp("trip_start_timestamp", TS_FORMAT).alias("trip_start_ts"),
        F.to_timestamp("trip_end_timestamp", TS_FORMAT).alias("trip_end_ts"),
        F.col("trip_seconds").cast("long").alias("trip_seconds"),
        num("trip_miles").alias("trip_miles"),
        F.col("pickup_community_area").cast("int").alias("pickup_community_area"),
        F.col("dropoff_community_area").cast("int").alias("dropoff_community_area"),
        F.col("pickup_census_tract").alias("pickup_census_tract"),
        F.col("dropoff_census_tract").alias("dropoff_census_tract"),
        num("fare").alias("fare"),
        num("tips").alias("tips"),
        num("tolls").alias("tolls"),
        num("extras").alias("extras"),
        num("trip_total").alias("trip_total"),
        F.initcap(F.trim("payment_type")).alias("payment_type"),
        F.trim("company").alias("company"),
        num("pickup_centroid_latitude").alias("pickup_lat"),
        num("pickup_centroid_longitude").alias("pickup_lon"),
        num("dropoff_centroid_latitude").alias("dropoff_lat"),
        num("dropoff_centroid_longitude").alias("dropoff_lon"),
        F.col("month").cast("string").alias("ingest_month"),
        "source_file",
    )


def with_reasons(df: DataFrame, start: date, end: date) -> DataFrame:
    """Adds `reasons`: every rule the row breaks (empty array = valid)."""
    lo, hi = F.lit(f"{start.isoformat()} 00:00:00").cast("timestamp"), F.lit(f"{end.isoformat()} 00:00:00").cast("timestamp")
    rules = [
        ("missing_trip_id", F.col("trip_id").isNull() | (F.col("trip_id") == "")),
        ("missing_start", F.col("trip_start_ts").isNull()),
        ("start_outside_window", F.col("trip_start_ts").isNotNull() & ((F.col("trip_start_ts") < lo) | (F.col("trip_start_ts") >= hi))),
        ("end_before_start", F.col("trip_end_ts").isNull() | (F.col("trip_end_ts") < F.col("trip_start_ts"))),
        ("bad_duration", F.col("trip_seconds").isNull() | (F.col("trip_seconds") <= 0) | (F.col("trip_seconds") > MAX_SECONDS)),
        ("bad_distance", F.col("trip_miles").isNull() | (F.col("trip_miles") < 0) | (F.col("trip_miles") > MAX_MILES)),
        ("bad_amount", F.col("trip_total").isNull() | (F.col("trip_total") < 0) | (F.col("fare") < 0) | (F.col("trip_total") > MAX_TOTAL)),
    ]
    return df.withColumn("reasons", F.array_compact(F.array(*[F.when(cond, F.lit(name)) for name, cond in rules])))


def dedup(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """One row per trip_id (the latest ingestion wins); the other copies are returned apart as duplicates."""
    w = Window.partitionBy("trip_id").orderBy(F.col("ingest_month").desc(), F.col("source_file").desc())
    ranked = df.withColumn("_rn", F.row_number().over(w))
    keep = ranked.filter("_rn = 1").drop("_rn")
    dups = ranked.filter("_rn > 1").drop("_rn").withColumn("reasons", F.array_union("reasons", F.array(F.lit("duplicate"))))
    return keep, dups


def enrich(df: DataFrame) -> DataFrame:
    """Columns the gold layer and the BI need."""
    return (
        df.withColumn("trip_date", F.to_date("trip_start_ts"))
        .withColumn("trip_hour", F.hour("trip_start_ts"))
        .withColumn("day_of_week", F.date_format("trip_start_ts", "E"))
        .withColumn("day_index", F.dayofweek("trip_start_ts"))  # 1 = Sunday … 7 = Saturday, for sorting
        .withColumn("duration_min", F.round(F.col("trip_seconds") / 60.0, 2))
        .withColumn("avg_speed_mph", F.when(F.col("trip_seconds") > 0, F.round(F.col("trip_miles") / (F.col("trip_seconds") / 3600.0), 2)))
        .withColumn("tip_rate", F.when(F.col("fare") > 0, F.round(F.col("tips") / F.col("fare"), 4)))
    )


def split(df: DataFrame, start: date, end: date) -> tuple[DataFrame, DataFrame]:
    """(clean, rejected): typed rows checked, deduplicated among the valid ones, enriched."""
    checked = with_reasons(typed(df), start, end)
    valid = checked.filter(F.size("reasons") == 0)
    invalid = checked.filter(F.size("reasons") > 0)
    clean, dups = dedup(valid)
    return enrich(clean).drop("reasons"), invalid.unionByName(dups)


def quality_report(total: int, clean: int, rejected: DataFrame, max_rate: float) -> dict:
    by_reason = {r["reason"]: r["n"] for r in rejected.select(F.explode("reasons").alias("reason")).groupBy("reason").agg(F.count("*").alias("n")).collect()}
    rate = (total - clean) / total if total else 0.0
    return {
        "run_at": datetime.now(timezone.utc).isoformat(),
        "rows_in": total,
        "rows_clean": clean,
        "rows_rejected": total - clean,
        "reject_rate": round(rate, 6),
        "max_reject_rate": max_rate,
        "rejected_by_reason": dict(sorted(by_reason.items(), key=lambda kv: -kv[1])),
        "passed": total > 0 and rate <= max_rate,
    }


def run(settings: Settings) -> dict:
    spark = spark_session("bronze_to_silver", settings)
    raw = read_bronze(spark, s3a(settings, BRONZE_TRIPS))
    raw.cache()
    total = raw.count()
    clean, rejected = split(raw, settings.trips_start, settings.trips_end)

    clean.repartition("trip_date").write.mode("overwrite").partitionBy("trip_date").parquet(s3a(settings, SILVER_TRIPS))
    rejected.write.mode("overwrite").partitionBy("ingest_month").parquet(s3a(settings, SILVER_REJECTED))

    n_clean = spark.read.parquet(s3a(settings, SILVER_TRIPS)).count()
    report = quality_report(total, n_clean, spark.read.parquet(s3a(settings, SILVER_REJECTED)), settings.max_reject_rate)
    s3 = storage.client(settings)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    storage.put_json(s3, settings.bucket, f"{SILVER_QUALITY}/run={stamp}.json", report)
    storage.put_json(s3, settings.bucket, f"{SILVER_QUALITY}/latest.json", report)
    log.info("quality: %s", json.dumps(report))
    spark.stop()
    if not report["passed"]:
        raise SystemExit(f"quality gate failed: reject rate {report['reject_rate']:.2%} > {settings.max_reject_rate:.0%} (or no row)")
    return report


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, stream=sys.stdout, format="%(asctime)s %(levelname)s %(message)s")
    print(json.dumps(run(Settings.from_env()), indent=2))
