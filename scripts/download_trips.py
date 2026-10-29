"""Standalone download of the same slice the DAG ingests, to local files (reproducibility, offline inspection).

    python scripts/download_trips.py --start 2023-01-01 --end 2023-04-01 --out data/raw

Writes data/raw/month=YYYY-MM/page=NNNNN.csv exactly as the SODA API returns them (same query as bronze ingestion).
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipeline.config import Settings  # noqa: E402
from pipeline.ingest.soda import csv_rows, fetch_page, month_windows, soda_params  # noqa: E402


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2023-01-01")
    ap.add_argument("--end", default="2023-04-01")
    ap.add_argument("--out", default="data/raw")
    ap.add_argument("--page-size", type=int, default=50000)
    a = ap.parse_args()
    settings = Settings(
        s3_endpoint="", s3_access_key="", s3_secret_key="", bucket="",
        soda_url=os.environ.get("SODA_DATASET_URL", "https://data.cityofchicago.org/resource/wrvz-psew.csv"),
        soda_token=os.environ.get("SODA_APP_TOKEN", ""),
        trips_start=date.fromisoformat(a.start), trips_end=date.fromisoformat(a.end),
        page_size=a.page_size, max_rows_per_month=None, max_reject_rate=1.0,
    )
    for start, end in month_windows(settings.trips_start, settings.trips_end):
        folder = Path(a.out) / f"month={start:%Y-%m}"
        folder.mkdir(parents=True, exist_ok=True)
        offset, page = 0, 0
        while True:
            body = fetch_page(settings, soda_params(start, end, a.page_size, offset))
            n = csv_rows(body)
            if not n:
                break
            page += 1
            (folder / f"page={page:05d}.csv").write_bytes(body)
            offset += n
            print(f"{start:%Y-%m} page {page}: {n} rows (total {offset})", flush=True)
            if n < a.page_size:
                break


if __name__ == "__main__":
    main()
