"""Silver and gold logic on small in-memory frames (local Spark, no object store)."""
from datetime import date

from pyspark.sql import functions as F

from pipeline.spark.bronze_to_silver import RAW_COLUMNS, RAW_SCHEMA, quality_report, split
from pipeline.spark.silver_to_gold import UNKNOWN_ZONE, build

START, END = date(2023, 1, 1), date(2023, 2, 1)


def raw_row(**over):
    base = {c: None for c in RAW_COLUMNS}
    base.update(
        trip_id="t1", taxi_id="x", trip_start_timestamp="2023-01-05T10:00:00.000", trip_end_timestamp="2023-01-05T10:15:00.000",
        trip_seconds="900", trip_miles="3.5", pickup_community_area="8", dropoff_community_area="32",
        fare="12.25", tips="2.50", tolls="0", extras="1", trip_total="15.75", payment_type="credit card", company="Flash Cab",
    )
    base.update(over)
    return base


def frame(spark, rows, month="2023-01"):
    df = spark.createDataFrame([tuple(r[c] for c in RAW_COLUMNS) for r in rows], RAW_SCHEMA)
    return df.withColumn("month", F.lit(month)).withColumn("source_file", F.lit(f"s3a://b/bronze/month={month}/page=00001.csv"))


def test_typing_and_derived_columns(spark):
    clean, rejected = split(frame(spark, [raw_row()]), START, END)
    assert rejected.count() == 0
    r = clean.collect()[0]
    assert str(r.trip_start_ts) == "2023-01-05 10:00:00"
    assert (r.trip_seconds, r.trip_miles, r.trip_total, r.pickup_community_area) == (900, 3.5, 15.75, 8)
    assert (str(r.trip_date), r.trip_hour, r.duration_min, r.avg_speed_mph, r.payment_type) == ("2023-01-05", 10, 15.0, 14.0, "Credit Card")
    assert r.tip_rate == round(2.5 / 12.25, 4)


def test_each_rule_rejects_with_its_reason_and_nothing_is_lost(spark):
    rows = [
        raw_row(trip_id="ok"),
        raw_row(trip_id=None),
        raw_row(trip_id="early", trip_start_timestamp="2022-12-31T23:00:00.000", trip_end_timestamp="2022-12-31T23:15:00.000"),
        raw_row(trip_id="back", trip_end_timestamp="2023-01-05T09:00:00.000"),
        raw_row(trip_id="zero", trip_seconds="0"),
        raw_row(trip_id="far", trip_miles="900"),
        raw_row(trip_id="neg", trip_total="-3"),
        raw_row(trip_id="garbled", trip_start_timestamp="not a date"),
    ]
    clean, rejected = split(frame(spark, rows), START, END)
    assert [r.trip_id for r in clean.collect()] == ["ok"]
    reasons = {r.trip_id: set(r.reasons) for r in rejected.collect()}
    assert reasons[None] == {"missing_trip_id"}
    assert reasons["early"] == {"start_outside_window"}
    assert reasons["back"] == {"end_before_start"}
    assert reasons["zero"] == {"bad_duration"}
    assert reasons["far"] == {"bad_distance"}
    assert reasons["neg"] == {"bad_amount"}
    assert "missing_start" in reasons["garbled"]
    assert clean.count() + rejected.count() == len(rows)


def test_duplicates_keep_the_latest_ingestion(spark):
    a = frame(spark, [raw_row(trip_id="d", fare="10")], month="2023-01")
    b = frame(spark, [raw_row(trip_id="d", fare="11")], month="2023-02")
    clean, rejected = split(a.unionByName(b), START, END)
    assert [r.fare for r in clean.collect()] == [11.0]
    assert [r.reasons for r in rejected.collect()] == [["duplicate"]]


def test_quality_report_and_gate(spark):
    clean, rejected = split(frame(spark, [raw_row(trip_id="ok"), raw_row(trip_id="z", trip_seconds="0")]), START, END)
    rep = quality_report(2, clean.count(), rejected, max_rate=0.2)
    assert (rep["rows_rejected"], rep["reject_rate"], rep["rejected_by_reason"], rep["passed"]) == (1, 0.5, {"bad_duration": 1}, False)
    assert quality_report(0, 0, rejected.limit(0), 0.2)["passed"] is False, "no row at all never passes"


def test_gold_tables(spark):
    rows = [
        raw_row(trip_id="a", pickup_community_area="8", trip_total="10", trip_seconds="600"),
        raw_row(trip_id="b", pickup_community_area="8", trip_total="20", trip_seconds="1200", payment_type="Cash"),
        raw_row(trip_id="c", pickup_community_area="32", trip_total="30", trip_seconds="1800", trip_start_timestamp="2023-01-06T08:00:00.000", trip_end_timestamp="2023-01-06T08:30:00.000"),
        raw_row(trip_id="d", pickup_community_area=None, trip_total="5"),
    ]
    clean, _ = split(frame(spark, rows), START, END)
    zones = spark.createDataFrame([(8, "Near North Side"), (32, "Loop")], "community_area INT, community_name STRING")
    g = build(clean, zones)
    daily = {str(r.trip_date): r for r in g["daily_kpis"].collect()}
    assert (daily["2023-01-05"].trips, daily["2023-01-05"].revenue, daily["2023-01-06"].revenue) == (3, 35.0, 30.0)
    assert daily["2023-01-05"].credit_card_share == round(2 / 3, 4)
    top = g["top_pickup_zones"].collect()
    assert (top[0].pickup_zone, top[0].trips, top[0].rank) == ("Near North Side", 2, 1)
    assert UNKNOWN_ZONE in {r.pickup_zone for r in top}
    assert sum(r.share_of_trips for r in top) == 1.0
    assert {r.payment_type: r.trips for r in g["payment_mix"].collect()} == {"Credit Card": 3, "Cash": 1}
    assert sum(r.trips for r in g["hourly_demand"].collect()) == 4
