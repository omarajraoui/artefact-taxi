"""Small native demo: Chicago CSV -> local bronze/silver/gold files, using DuckDB.

This is intentionally separate from the Docker/Airflow/Spark pipeline. It uses
the same source, quality rules, table names and dashboard, but runs on one PC.
"""
from __future__ import annotations

import argparse
import csv
import http.client
import io
import json
import shutil
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, datetime, timezone
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
SOURCE = "https://data.cityofchicago.org/resource/wrvz-psew.csv"


def month_windows(start: date, end: date):
    current = start
    while current < end:
        next_month = date(current.year + (current.month == 12), current.month % 12 + 1, 1)
        stop = min(next_month, end)
        yield current, stop
        current = stop


def fetch_page(start: date, end: date, limit: int, offset: int) -> bytes:
    params = {
        "$where": f"trip_start_timestamp >= '{start}T00:00:00' AND trip_start_timestamp < '{end}T00:00:00'",
        "$order": ":id", "$limit": limit, "$offset": offset,
    }
    url = SOURCE + "?" + urllib.parse.urlencode(params)
    for attempt in range(1, 7):
        try:
            with urllib.request.urlopen(url, timeout=120) as response:
                return response.read()
        except (urllib.error.URLError, TimeoutError, OSError, http.client.IncompleteRead) as exc:
            if isinstance(exc, urllib.error.HTTPError) and exc.code not in (429, 500, 502, 503, 504):
                raise
            if attempt == 6:
                raise RuntimeError(f"Chicago API did not respond after 6 attempts: {exc}") from exc
            print(f"  API request failed ({exc}); retry {attempt}/6", flush=True)
            time.sleep(min(30, 2**attempt))
    raise AssertionError("unreachable")


def download(root: Path, start: date, end: date, cap: int) -> None:
    bronze = root / "bronze" / "taxi_trips"
    for begin, stop in month_windows(start, end):
        folder = bronze / f"month={begin:%Y-%m}"
        manifest = folder / "_manifest.json"
        if manifest.exists():
            previous = json.loads(manifest.read_text())
            if previous.get("requested_cap") == cap and previous.get("window") == [str(begin), str(stop)]:
                print(f"{begin:%Y-%m}: already downloaded; reusing files")
                continue
        if folder.exists():
            shutil.rmtree(folder)
        folder.mkdir(parents=True)
        rows = 0
        page = 0
        while rows < cap:
            limit = min(50000, cap - rows)
            body = fetch_page(begin, stop, limit, rows)
            count = max(0, sum(1 for _ in csv.reader(io.StringIO(body.decode("utf-8-sig")))) - 1)
            if count == 0:
                break
            page += 1
            (folder / f"page={page:05d}.csv").write_bytes(body)
            rows += count
            print(f"{begin:%Y-%m}: {rows:,} rows downloaded", flush=True)
            if count < limit:
                break
        manifest.write_text(json.dumps({"window": [str(begin), str(stop)], "rows": rows, "pages": page,
                                        "requested_cap": cap}, indent=2))
    zones = root / "bronze" / "reference" / "community_areas"
    zones.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(ROOT / "seeds" / "community_areas.csv", zones / "community_areas.csv")


def quoted(value: Path | str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def make_parquet(con: duckdb.DuckDBPyConnection, query: str, target: Path) -> int:
    target.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"COPY ({query}) TO {quoted(target)} (FORMAT PARQUET)")
    return con.execute(f"SELECT count(*) FROM read_parquet({quoted(target)})").fetchone()[0]


def transform(root: Path, start: date, end: date, max_reject_rate: float) -> None:
    paths = [path for begin, _ in month_windows(start, end)
             for path in sorted((root / "bronze" / "taxi_trips" / f"month={begin:%Y-%m}").glob("page=*.csv"))]
    if not paths:
        raise RuntimeError("No CSV files were downloaded. Check your internet connection.")
    con = duckdb.connect()
    path_list = "[" + ", ".join(quoted(p) for p in paths) + "]"
    con.execute(f"CREATE TEMP TABLE raw AS SELECT * FROM read_csv({path_list}, header=true, all_varchar=true, union_by_name=true, filename=true)")
    con.execute("""
        CREATE TEMP TABLE typed AS SELECT
            NULLIF(trim(trip_id), '') AS trip_id,
            try_cast(trip_start_timestamp AS TIMESTAMP) AS trip_start_ts,
            try_cast(trip_end_timestamp AS TIMESTAMP) AS trip_end_ts,
            try_cast(trip_seconds AS BIGINT) AS trip_seconds,
            try_cast(trip_miles AS DOUBLE) AS trip_miles,
            try_cast(pickup_community_area AS INTEGER) AS pickup_community_area,
            try_cast(fare AS DOUBLE) AS fare,
            try_cast(tips AS DOUBLE) AS tips,
            try_cast(trip_total AS DOUBLE) AS trip_total,
            CASE WHEN lower(trim(payment_type)) = 'credit card' THEN 'Credit Card'
                 ELSE upper(substr(trim(payment_type), 1, 1)) || lower(substr(trim(payment_type), 2))
            END AS payment_type,
            filename AS source_file
        FROM raw
    """)
    con.execute(f"""
        CREATE TEMP TABLE checked AS SELECT *,
            (trip_id IS NULL) AS missing_trip_id,
            (trip_start_ts IS NULL) AS missing_start,
            (trip_start_ts IS NOT NULL AND (trip_start_ts < TIMESTAMP {quoted(str(start))}
                OR trip_start_ts >= TIMESTAMP {quoted(str(end))})) AS start_outside_window,
            (trip_end_ts IS NULL OR trip_end_ts < trip_start_ts) AS end_before_start,
            (trip_seconds IS NULL OR trip_seconds <= 0 OR trip_seconds > 86400) AS bad_duration,
            (trip_miles IS NULL OR trip_miles < 0 OR trip_miles > 500) AS bad_distance,
            (trip_total IS NULL OR trip_total < 0 OR fare < 0 OR trip_total > 1000) AS bad_amount
        FROM typed
    """)
    rules = ["missing_trip_id", "missing_start", "start_outside_window", "end_before_start", "bad_duration", "bad_distance", "bad_amount"]
    valid = " AND ".join(f"NOT coalesce({r}, false)" for r in rules)
    con.execute(f"CREATE TEMP TABLE valid AS SELECT *, row_number() OVER (PARTITION BY trip_id ORDER BY source_file DESC) AS rn FROM checked WHERE {valid}")
    total = con.execute("SELECT count(*) FROM checked").fetchone()[0]
    clean = con.execute("SELECT count(*) FROM valid WHERE rn=1").fetchone()[0]
    if total == 0:
        raise RuntimeError("The API returned no trips for this period.")
    rejected = total - clean
    by_reason = {r: con.execute(f"SELECT count(*) FROM checked WHERE {r}").fetchone()[0] for r in rules}
    by_reason["duplicate"] = con.execute("SELECT count(*) FROM valid WHERE rn>1").fetchone()[0]
    by_reason = {key: value for key, value in by_reason.items() if value}
    report = {
        "run_at": datetime.now(timezone.utc).isoformat(), "rows_in": total, "rows_clean": clean,
        "rows_rejected": rejected, "reject_rate": round(rejected / total, 6),
        "max_reject_rate": max_reject_rate, "rejected_by_reason": by_reason,
        "passed": rejected / total <= max_reject_rate,
    }
    quality = root / "silver" / "_quality" / "latest.json"
    quality.parent.mkdir(parents=True, exist_ok=True)
    quality.write_text(json.dumps(report, indent=2))
    print(f"Quality: {clean:,} clean / {total:,} rows; {rejected / total:.1%} rejected")
    if not report["passed"]:
        raise RuntimeError(f"Quality check failed: rejection rate exceeds {max_reject_rate:.0%}")
    con.execute("""
        CREATE TEMP TABLE trips AS SELECT
            trip_id, trip_start_ts, CAST(trip_start_ts AS DATE) AS trip_date,
            extract('hour' FROM trip_start_ts)::INTEGER AS trip_hour,
            (extract('dow' FROM trip_start_ts)::INTEGER + 1) AS day_index,
            strftime(trip_start_ts, '%a') AS day_of_week,
            trip_miles, pickup_community_area, fare, tips, trip_total, payment_type,
            round(trip_seconds / 60.0, 2) AS duration_min,
            CASE WHEN fare > 0 THEN round(tips / fare, 4) END AS tip_rate
        FROM valid WHERE rn=1
    """)
    make_parquet(con, "SELECT * FROM trips", root / "silver" / "taxi_trips" / "data.parquet")
    con.execute(f"CREATE TEMP VIEW zones AS SELECT * FROM read_csv({quoted(root / 'bronze' / 'reference' / 'community_areas' / 'community_areas.csv')}, header=true)")
    gold = root / "gold"
    queries = {
        "daily_kpis": """SELECT trip_date, count(*) AS trips, round(sum(trip_total),2) AS revenue,
            round(sum(fare),2) AS fares, round(sum(tips),2) AS tips,
            round(avg(trip_total),2) AS avg_trip_total, round(avg(duration_min),2) AS avg_duration_min,
            round(median(duration_min),2) AS median_duration_min,
            round(quantile_cont(duration_min,0.9),2) AS p90_duration_min,
            round(avg(trip_miles),2) AS avg_miles,
            round(avg(CASE WHEN payment_type='Credit Card' THEN 1.0 ELSE 0.0 END),4) AS credit_card_share
            FROM trips GROUP BY trip_date ORDER BY trip_date""",
        "top_pickup_zones": """WITH counts AS (SELECT pickup_community_area, count(*) AS trips,
            round(sum(trip_total),2) AS revenue, round(avg(duration_min),2) AS avg_duration_min,
            round(avg(trip_total),2) AS avg_trip_total FROM trips GROUP BY pickup_community_area)
            SELECT c.pickup_community_area, coalesce(z.community_name, 'Unknown / outside Chicago') AS pickup_zone,
            c.trips, c.revenue, c.avg_duration_min, c.avg_trip_total,
            round(c.trips::DOUBLE / (SELECT count(*) FROM trips),4) AS share_of_trips,
            dense_rank() OVER (ORDER BY c.trips DESC) AS rank
            FROM counts c LEFT JOIN zones z ON c.pickup_community_area=z.community_area ORDER BY rank""",
        "hourly_demand": """SELECT day_index, day_of_week, trip_hour, count(*) AS trips,
            round(avg(duration_min),2) AS avg_duration_min, round(avg(trip_total),2) AS avg_trip_total
            FROM trips GROUP BY day_index, day_of_week, trip_hour ORDER BY day_index, trip_hour""",
        "payment_mix": """SELECT coalesce(payment_type,'Unknown') AS payment_type, count(*) AS trips,
            round(sum(trip_total),2) AS revenue, round(avg(tip_rate),4) AS avg_tip_rate
            FROM trips GROUP BY 1 ORDER BY trips DESC""",
    }
    for name, query in queries.items():
        count = make_parquet(con, query, gold / name / "data.parquet")
        print(f"Gold: {name} ({count} rows)")
    con.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the taxi dashboard locally, without Docker or S3")
    parser.add_argument("--start", type=date.fromisoformat, default=date(2023, 1, 1))
    parser.add_argument("--end", type=date.fromisoformat, default=date(2023, 4, 1))
    parser.add_argument("--rows-per-month", type=int, default=20000)
    parser.add_argument("--data-dir", type=Path, default=ROOT / "data" / "local")
    parser.add_argument("--max-reject-rate", type=float, default=0.2)
    args = parser.parse_args()
    if args.start >= args.end or args.rows_per_month < 1:
        parser.error("start must precede end and rows-per-month must be positive")
    root = args.data_dir.resolve()
    download(root, args.start, args.end, args.rows_per_month)
    transform(root, args.start, args.end, args.max_reject_rate)
    print(f"Done. Start the dashboard with: python -m uvicorn api.main:app --host 127.0.0.1 --port 8000")
    print("Set STORAGE_BACKEND=local and DATA_DIR to the path shown in the guide first.")


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, duckdb.Error) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)
