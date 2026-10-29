"""Settings read from the environment (see .env.example), and the datalake's layout in one place."""
from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date


def _env(name: str, default: str | None = None) -> str:
    value = os.environ.get(name, default)
    if value is None:
        raise RuntimeError(f"missing environment variable {name}")
    return value


@dataclass(frozen=True)
class Settings:
    s3_endpoint: str
    s3_access_key: str
    s3_secret_key: str
    bucket: str
    soda_url: str
    soda_token: str
    trips_start: date
    trips_end: date
    page_size: int
    max_rows_per_month: int | None
    max_reject_rate: float

    @staticmethod
    def from_env() -> "Settings":
        cap = os.environ.get("MAX_ROWS_PER_MONTH", "").strip()
        return Settings(
            s3_endpoint=_env("S3_ENDPOINT", "http://s3:8333"),
            s3_access_key=_env("S3_ACCESS_KEY"),
            s3_secret_key=_env("S3_SECRET_KEY"),
            bucket=_env("S3_BUCKET", "datalake"),
            soda_url=_env("SODA_DATASET_URL", "https://data.cityofchicago.org/resource/wrvz-psew.csv"),
            soda_token=os.environ.get("SODA_APP_TOKEN", ""),
            trips_start=date.fromisoformat(_env("TRIPS_START", "2023-01-01")),
            trips_end=date.fromisoformat(_env("TRIPS_END", "2023-04-01")),
            page_size=int(_env("SODA_PAGE_SIZE", "50000")),
            max_rows_per_month=int(cap) if cap else None,
            max_reject_rate=float(_env("MAX_REJECT_RATE", "0.2")),
        )


# Datalake layout (keys inside the bucket). One place, used by ingestion, Spark jobs and the API.
BRONZE_TRIPS = "bronze/taxi_trips"            # month=YYYY-MM/page=NNNNN.csv + _manifest.json (raw, untouched)
BRONZE_ZONES = "bronze/reference/community_areas"
SILVER_TRIPS = "silver/taxi_trips"            # Parquet, partitioned by trip_date
SILVER_REJECTED = "silver/taxi_trips_rejected"  # rows failing a quality rule, with the reason
SILVER_QUALITY = "silver/_quality"            # one JSON report per run
GOLD = "gold"                                 # gold/<table>/ Parquet


def s3a(settings: Settings, key: str) -> str:
    return f"s3a://{settings.bucket}/{key}"
