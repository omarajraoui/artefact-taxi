# Chicago Taxi Trips — a local medallion datalake

End-to-end, orchestrated, reproducible pipeline: raw taxi trips from the City of Chicago's SODA API land in an object
store (**bronze**), are typed, deduplicated and quality-checked with **PySpark** (**silver**), then aggregated into
BI-ready tables (**gold**), exposed over SQL/JSON and a small dashboard. Orchestrated by **Airflow**, everything runs
with **Docker Compose**.

> 📘 Full explanation in French (why, how it was built, Docker, Docker Compose, Git/GitHub): [`docs/GUIDE.md`](docs/GUIDE.md)

## Quick start

Prerequisites: Docker with Compose v2, ~6 GB of RAM free for Docker, ~5 GB of disk, internet access (SODA API, image builds).

```bash
cp .env.example .env          # defaults are fine
docker compose up -d --build  # or: make up
```

That's it: the `taxi_pipeline` DAG is unpaused at creation and scheduled `@once`, so it starts by itself as soon as
the scheduler is up. With the default period (Q1 2023, 1.45 M trips) a full run takes roughly 15–30 min, most of it
the download (SODA serves ~50 000 rows per call).

| What | Where |
|---|---|
| Airflow UI (DAG, logs) | http://localhost:8080 — `admin` / `admin` |
| Datalake browser (SeaweedFS filer) | http://localhost:8888/buckets/datalake/ |
| Dashboard over gold | http://localhost:8000 |
| API docs (JSON + SQL endpoint) | http://localhost:8000/docs |

Run it again: `make trigger` (or the ▶ button in Airflow). Re-download the months: trigger with
`{"force_ingest": true}`. Quick demo on a small slice: set `MAX_ROWS_PER_MONTH=20000` in `.env` before `up`.

```bash
make status   # DAG runs
make logs     # scheduler logs
make test     # unit tests of the Spark logic, inside the Airflow image
make down     # stop (data kept) — make clean: stop and delete the volumes
```

Example SQL on gold:

```bash
curl -s localhost:8000/sql -H 'content-type: application/json' \
  -d '{"query": "select pickup_zone, trips, revenue from top_pickup_zones order by trips desc limit 5"}'
```

## Data: period and download

* Source: [Taxi Trips (2013-2023)](https://data.cityofchicago.org/Transportation/Taxi-Trips-2013-2023-/wrvz-psew),
  SODA endpoint `https://data.cityofchicago.org/resource/wrvz-psew.csv`.
* Period: **2023-01-01 → 2023-04-01 (Q1 2023, half-open)**: **1,446,439 trips**
  (`$select=count(*)` on that filter, 29 Oct 2026). Change `TRIPS_START` / `TRIPS_END` in `.env`.
* Query per page: `$where=trip_start_timestamp >= '…' AND trip_start_timestamp < '…'`, `$order=:id` (stable paging),
  `$limit=50000`, `$offset` incremented; one month per Airflow task, retries with backoff on 429/5xx.
  An app token (`SODA_APP_TOKEN`) is optional and raises rate limits.
* Same slice to local files, without the stack:
  `python scripts/download_trips.py --start 2023-01-01 --end 2023-04-01 --out data/raw`

## Architecture

```
            SODA API (CSV pages)                       seeds/community_areas.csv
                    │  Airflow: ingest_month × N (dynamic mapping)       │ ingest_reference
                    ▼                                                    ▼
 ┌──────────────────────────────────────── SeaweedFS  s3://datalake ─────────────────────────────────────┐
 │ BRONZE  bronze/taxi_trips/month=YYYY-MM/page=NNNNN.csv + _manifest.json   (raw, as received)        │
 │         bronze/reference/community_areas/community_areas.csv                                         │
 │              │  Spark bronze_to_silver: types, rules, dedup, enrichment, quality gate               │
 │              ▼                                                                                       │
 │ SILVER  silver/taxi_trips/trip_date=…/ (Parquet)   silver/taxi_trips_rejected/ (with reasons)       │
 │         silver/_quality/latest.json, run=….json                                                     │
 │              │  Spark silver_to_gold: joins + business aggregates                                   │
 │              ▼                                                                                       │
 │ GOLD    gold/daily_kpis  gold/top_pickup_zones  gold/hourly_demand  gold/payment_mix  (Parquet)     │
 └─────────────────────────────────────────────────────────────────────────────────────────────────────┘
                    │  DuckDB (httpfs) reads gold Parquet in place
                    ▼
          FastAPI: /kpis/{table}, /sql, /quality  +  dashboard (Chart.js)
```

DAG `taxi_pipeline`:

```
plan_months ─► ingest_month[2023-01, 2023-02, 2023-03] ─► check_bronze ─┐
                                              ingest_reference ──────────┴─► bronze_to_silver ─► silver_to_gold
```

### Layers

**Bronze** — the API's CSV pages stored byte for byte, partitioned by month. A `_manifest.json` (query, pages, rows,
timing) is written last: its presence marks the month complete, so re-runs skip it (idempotent), and a partial month
is wiped and re-ingested.

**Silver** (`pipeline/spark/bronze_to_silver.py`) — explicit all-text schema, then typing (timestamps
`yyyy-MM-dd'T'HH:mm:ss.SSS`, numerics, community areas). Rules, each giving a named reason:
`missing_trip_id`, `missing_start`, `start_outside_window`, `end_before_start`, `bad_duration` (≤ 0 or > 24 h),
`bad_distance` (< 0 or > 500 mi), `bad_amount` (null/negative total or fare, total > $1 000). Valid rows are
deduplicated on `trip_id` (latest ingestion wins; extra copies are rejected as `duplicate`) and enriched
(`trip_date`, `trip_hour`, `day_of_week`, `duration_min`, `avg_speed_mph`, `tip_rate`). Rejected rows are kept in
`silver/taxi_trips_rejected` with all their reasons; a quality report is written, and the task **fails if more than
`MAX_REJECT_RATE` (20 %) of rows are rejected** — a broken source must stop the pipeline, not reach the BI.

**Gold** (`pipeline/spark/silver_to_gold.py`):

| Table | Grain | Columns |
|---|---|---|
| `daily_kpis` | day | trips, revenue, fares, tips, avg trip total, avg / median / p90 duration, avg miles, card share |
| `top_pickup_zones` | pickup community area | zone name (joined with the reference), trips, share, revenue, avg duration, avg total, rank |
| `hourly_demand` | day of week × hour | trips, avg duration, avg total |
| `payment_mix` | payment type | trips, revenue, avg tip rate |

Measured on a real two-day sample (2–3 Jan 2023, 22 714 rows, same code, local Spark): 2.2 % rejected
(479 `bad_duration`, 16 `bad_amount`, 7 `bad_distance`, 3 `end_before_start`); O'Hare is the top pickup zone (22.8 %).

## Full run (measured, 29 Oct 2026, 4 vCPU / 5.8 GB VM)

`docker compose up -d --build`, DAG started by itself:

| Step | Result | Time |
|---|---|---|
| ingest_month × 3 | 1,446,439 rows in 30 SODA pages (Jan / Feb / Mar in parallel) | ~3 min |
| bronze_to_silver | 1,416,392 clean, 30,047 rejected (2.08 %): 28,767 `bad_duration`, 1,347 `bad_amount`, 36 `end_before_start`, 25 `bad_distance`; gate passed | 3 min 30 s |
| silver_to_gold | daily_kpis 90 rows, top_pickup_zones 78, hourly_demand 168, payment_mix 7 | 1 min 10 s |

Q1 2023 on clean trips: $36.46 M revenue, 19.4 min average duration; top pickup zones: Near North Side (21.1 %),
O'Hare (16.9 %, highest revenue), Loop (15.9 %). Peak memory stayed under each container's cap.

## Technical choices and trade-offs

* **Airflow 2.10, LocalExecutor + Postgres** — the most common orchestrator in client contexts; dynamic task mapping
  gives one retryable, parallel task per month (max 3 at once to stay polite with the API). Celery/Kubernetes
  executors would be overkill locally.
* **Spark in local mode inside the Airflow image (`spark-submit` from a BashOperator)** rather than a Spark cluster in
  Compose: one fewer moving part and less RAM for a few million rows; the jobs are plain `spark-submit` scripts, so
  pointing `SPARK_MASTER` at a cluster (or using `SparkSubmitOperator`) needs no code change.
* **SeaweedFS** as the S3-compatible object store (MinIO was the first choice, but its free Docker images and binaries
  were withdrawn in 2025: `docker pull minio/minio` is denied, `dl.min.io` answers 410), accessed by Spark through S3A (`hadoop-aws` jars baked into the image,
  no download at run time) and by boto3 for ingestion. Switching to AWS S3/GCS is configuration.
* **Parquet** for silver/gold (columnar, typed, compressed). Silver partitioned by `trip_date`; each run rewrites the window it covers. Writes use the
  **S3A magic committer** (no rename on the object store). Gold tables are small: one file each.
* **Raw kept untouched**: bronze is the API's own CSV, so any silver rule can be changed and replayed without
  re-downloading.
* **Quality as data**: rejects are stored with their reasons and counted, not silently filtered; the gate threshold is
  configurable.
* **Timestamps**: the source publishes Chicago wall-clock time without a zone; Spark runs in UTC so values are kept as
  published (no hidden shift).
* **Community areas** come from a versioned seed (77 official areas) rather than a second API, so the join is
  reproducible; trips outside Chicago (null area) are kept as `Unknown / outside Chicago`.
* **Exposition with DuckDB + FastAPI**: SQL straight on the gold Parquet in SeaweedFS, no extra database to load; the SQL
  endpoint is read-only (single SELECT/WITH).

## With more time

* Table format with ACID and time travel (**Delta Lake / Iceberg**) for silver and gold, `MERGE` for late updates.
* Incremental runs: a daily schedule ingesting only new days (watermark on `trip_start_timestamp`), backfills by
  interval.
* Data contracts and richer checks (Great Expectations / Soda Core), alerting on the quality report.
* Spark cluster (or Spark on Kubernetes) and a bigger period; tuning partitions and file sizes.
* dbt on top of gold for the semantic layer; Metabase/Superset dashboard; lineage with OpenLineage/Marquez.
* CI (lint, unit tests, a DAG import test, an end-to-end smoke run with `MAX_ROWS_PER_MONTH`), secrets out of `.env`.
* Geo: census tracts and centroid points for heatmaps; weather enrichment.

## Repository

```
docker-compose.yml          SeaweedFS, Postgres, Airflow (init, webserver, scheduler), API
.env.example                every setting (period, storage, quality threshold)
docker/airflow/Dockerfile   Airflow + Java + PySpark + S3A jars
docker/api/Dockerfile       FastAPI + DuckDB (httpfs)
dags/taxi_pipeline.py       the DAG
pipeline/config.py          settings and datalake layout
pipeline/storage.py         S3 helpers (boto3)
pipeline/ingest/soda.py     bronze ingestion (paging, retries, manifests)
pipeline/spark/             session, bronze_to_silver, silver_to_gold
seeds/community_areas.csv   77 Chicago community areas
api/                        exposition (JSON, SQL, dashboard)
scripts/download_trips.py   standalone download of the same slice
tests/                      unit tests (paging, cleaning rules, dedup, quality gate, gold tables)
```

Tests: `make test` in Docker, or locally `pip install -r requirements-dev.txt && pytest -q` (needs Java 17+).
