"""Bronze ingestion: Chicago Taxi Trips from the Socrata (SODA) API, one month at a time, stored as received.

Each month lands under bronze/taxi_trips/month=YYYY-MM/ as the API's own CSV pages (page=00001.csv, ...), plus a
_manifest.json written last (query, row counts, timing). A month with a complete manifest is skipped: re-running the
DAG does not download it again. `force=True` deletes the month and ingests it from scratch.
"""
from __future__ import annotations

import csv
import io
import logging
import time
from datetime import date, datetime, timezone

import requests

from pipeline import storage
from pipeline.config import BRONZE_TRIPS, BRONZE_ZONES, Settings

log = logging.getLogger(__name__)


def month_windows(start: date, end: date) -> list[tuple[date, date]]:
    """Half-open monthly windows covering [start, end): 2023-01-15..2023-03-01 → [01-15, 02-01), [02-01, 03-01)."""
    out = []
    cur = start
    while cur < end:
        nxt = date(cur.year + (cur.month == 12), cur.month % 12 + 1, 1)
        out.append((cur, min(nxt, end)))
        cur = nxt
    return out


def soda_params(start: date, end: date, limit: int, offset: int) -> dict[str, str]:
    """A stable page: filtered on trip_start_timestamp, ordered by the row id (:id) so offsets never shift."""
    return {
        "$where": f"trip_start_timestamp >= '{start.isoformat()}T00:00:00' AND trip_start_timestamp < '{end.isoformat()}T00:00:00'",
        "$order": ":id",
        "$limit": str(limit),
        "$offset": str(offset),
    }


def csv_rows(body: bytes) -> int:
    """Data rows in a CSV page (header excluded; quoted newlines handled)."""
    return max(0, sum(1 for _ in csv.reader(io.StringIO(body.decode("utf-8")))) - 1)


def fetch_page(settings: Settings, params: dict[str, str], tries: int = 6) -> bytes:
    headers = {"X-App-Token": settings.soda_token} if settings.soda_token else {}
    for attempt in range(1, tries + 1):
        try:
            r = requests.get(settings.soda_url, params=params, headers=headers, timeout=300)
            if r.status_code == 200:
                return r.content
            if r.status_code not in (429, 500, 502, 503, 504):
                r.raise_for_status()
            log.warning("SODA answered %s (try %d/%d)", r.status_code, attempt, tries)
        except (requests.ConnectionError, requests.exceptions.ChunkedEncodingError) as e:
            log.warning("SODA unreachable (try %d/%d): %s", attempt, tries, e)
        except requests.Timeout:
            log.warning("SODA timed out (try %d/%d)", attempt, tries)
        time.sleep(min(60, 2**attempt))
    raise RuntimeError(f"SODA page failed after {tries} tries: {params}")


def ingest_month(settings: Settings, start: date, end: date, force: bool = False) -> dict:
    """Downloads one month into bronze. Returns its manifest."""
    s3 = storage.client(settings)
    prefix = f"{BRONZE_TRIPS}/month={start:%Y-%m}/"
    manifest_key = prefix + "_manifest.json"
    done = storage.get_json(s3, settings.bucket, manifest_key)
    if done and done.get("complete") and done.get("window") == [start.isoformat(), end.isoformat()] and not force:
        log.info("month %s already ingested (%s rows): skipped", f"{start:%Y-%m}", done["rows"])
        return done
    removed = storage.delete_prefix(s3, settings.bucket, prefix)  # a partial or forced month restarts clean
    if removed:
        log.info("removed %d old objects under %s", removed, prefix)

    t0 = time.time()
    rows, page, offset = 0, 0, 0
    cap = settings.max_rows_per_month
    while True:
        limit = settings.page_size if cap is None else min(settings.page_size, cap - rows)
        if limit <= 0:
            break
        body = fetch_page(settings, soda_params(start, end, limit, offset))
        n = csv_rows(body)
        if n == 0:
            break
        page += 1
        storage.put_bytes(s3, settings.bucket, f"{prefix}page={page:05d}.csv", body)
        rows += n
        offset += n
        log.info("month %s: page %d, %d rows (total %d)", f"{start:%Y-%m}", page, n, rows)
        if n < limit:
            break

    manifest = {
        "source": settings.soda_url,
        "window": [start.isoformat(), end.isoformat()],
        "query": soda_params(start, end, settings.page_size, 0) | {"$offset": "paged"},
        "pages": page,
        "rows": rows,
        "capped_at": cap,
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "seconds": round(time.time() - t0, 1),
        "complete": True,
    }
    storage.put_json(s3, settings.bucket, manifest_key, manifest)  # written last: its presence means "month complete"
    return manifest


def ingest_reference(settings: Settings, seeds_dir: str = "/opt/airflow/seeds") -> None:
    """The 77 Chicago community areas (number → name), a static reference copied into bronze."""
    s3 = storage.client(settings)
    with open(f"{seeds_dir}/community_areas.csv", "rb") as f:
        storage.put_bytes(s3, settings.bucket, f"{BRONZE_ZONES}/community_areas.csv", f.read())
