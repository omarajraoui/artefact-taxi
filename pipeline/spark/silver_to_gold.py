"""Gold: business tables for the BI team, built from silver and the community-area reference.

Tables (gold/<name>/, Parquet, one small file each):
  daily_kpis         — per day: trips, revenue, fares, tips, average/median/p90 duration, distance, card share
  top_pickup_zones   — per pickup community area (named): trips, revenue, average duration and fare, rank
  hourly_demand      — per day of week × hour: trips, average duration and total
  payment_mix        — per payment type: trips, revenue, average tip rate
Run: spark-submit pipeline/spark/silver_to_gold.py
"""
from __future__ import annotations

import logging
import sys

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from pipeline.config import BRONZE_ZONES, GOLD, SILVER_TRIPS, Settings, s3a
from pipeline.spark.session import spark_session

log = logging.getLogger(__name__)
UNKNOWN_ZONE = "Unknown / outside Chicago"


def read_zones(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.option("header", "true").schema("community_area INT, community_name STRING").csv(path)


def daily_kpis(trips: DataFrame) -> DataFrame:
    return (
        trips.groupBy("trip_date")
        .agg(
            F.count("*").alias("trips"),
            F.round(F.sum("trip_total"), 2).alias("revenue"),
            F.round(F.sum("fare"), 2).alias("fares"),
            F.round(F.sum("tips"), 2).alias("tips"),
            F.round(F.avg("trip_total"), 2).alias("avg_trip_total"),
            F.round(F.avg("duration_min"), 2).alias("avg_duration_min"),
            F.round(F.percentile_approx("duration_min", 0.5, 10000), 2).alias("median_duration_min"),
            F.round(F.percentile_approx("duration_min", 0.9, 10000), 2).alias("p90_duration_min"),
            F.round(F.avg("trip_miles"), 2).alias("avg_miles"),
            F.round(F.avg(F.when(F.col("payment_type") == "Credit Card", 1.0).otherwise(0.0)), 4).alias("credit_card_share"),
        )
        .orderBy("trip_date")
    )


def top_pickup_zones(trips: DataFrame, zones: DataFrame) -> DataFrame:
    agg = trips.groupBy("pickup_community_area").agg(
        F.count("*").alias("trips"),
        F.round(F.sum("trip_total"), 2).alias("revenue"),
        F.round(F.avg("duration_min"), 2).alias("avg_duration_min"),
        F.round(F.avg("trip_total"), 2).alias("avg_trip_total"),
    )
    named = (
        agg.join(F.broadcast(zones), agg.pickup_community_area == zones.community_area, "left")
        .select(
            "pickup_community_area",
            F.coalesce("community_name", F.lit(UNKNOWN_ZONE)).alias("pickup_zone"),
            "trips", "revenue", "avg_duration_min", "avg_trip_total",
        )
    )
    total = trips.count()
    return (
        named.withColumn("share_of_trips", F.round(F.col("trips") / F.lit(total), 4))
        .withColumn("rank", F.dense_rank().over(Window.orderBy(F.col("trips").desc())))
        .orderBy("rank")
    )


def hourly_demand(trips: DataFrame) -> DataFrame:
    return (
        trips.groupBy("day_index", "day_of_week", "trip_hour")
        .agg(
            F.count("*").alias("trips"),
            F.round(F.avg("duration_min"), 2).alias("avg_duration_min"),
            F.round(F.avg("trip_total"), 2).alias("avg_trip_total"),
        )
        .orderBy("day_index", "trip_hour")
    )


def payment_mix(trips: DataFrame) -> DataFrame:
    return (
        trips.groupBy(F.coalesce("payment_type", F.lit("Unknown")).alias("payment_type"))
        .agg(
            F.count("*").alias("trips"),
            F.round(F.sum("trip_total"), 2).alias("revenue"),
            F.round(F.avg("tip_rate"), 4).alias("avg_tip_rate"),
        )
        .orderBy(F.col("trips").desc())
    )


def build(trips: DataFrame, zones: DataFrame) -> dict[str, DataFrame]:
    return {
        "daily_kpis": daily_kpis(trips),
        "top_pickup_zones": top_pickup_zones(trips, zones),
        "hourly_demand": hourly_demand(trips),
        "payment_mix": payment_mix(trips),
    }


def run(settings: Settings) -> dict[str, int]:
    spark = spark_session("silver_to_gold", settings)
    trips = spark.read.parquet(s3a(settings, SILVER_TRIPS))
    trips.cache()
    zones = read_zones(spark, s3a(settings, BRONZE_ZONES))
    counts = {}
    for name, df in build(trips, zones).items():
        df.coalesce(1).write.mode("overwrite").parquet(s3a(settings, f"{GOLD}/{name}"))
        counts[name] = spark.read.parquet(s3a(settings, f"{GOLD}/{name}")).count()
        log.info("gold %s: %d rows", name, counts[name])
    spark.stop()
    return counts


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, stream=sys.stdout, format="%(asctime)s %(levelname)s %(message)s")
    print(run(Settings.from_env()))
