"""Exposition layer: the gold tables over SQL (DuckDB reading Parquet straight from MinIO), as JSON and a small dashboard.

GET /                     dashboard (daily revenue, duration, top zones)
GET /health               storage reachable, gold tables present
GET /tables               gold tables and their row counts
GET /kpis/{table}?limit=  a gold table as JSON
GET /quality              the latest silver quality report
POST /sql {"query": ...}  read-only SQL over the gold views (SELECT / WITH only)
"""
from __future__ import annotations

import json
import os
import re
import threading

import boto3
import duckdb
from botocore.client import Config
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from pydantic import BaseModel

BUCKET = os.environ.get("S3_BUCKET", "datalake")
ENDPOINT = os.environ.get("S3_ENDPOINT", "http://s3:8333")
KEY, SECRET = os.environ["S3_ACCESS_KEY"], os.environ["S3_SECRET_KEY"]
TABLES = ["daily_kpis", "top_pickup_zones", "hourly_demand", "payment_mix"]

app = FastAPI(title="Chicago Taxi — gold layer")
_lock = threading.Lock()  # one DuckDB connection, used one query at a time


def _connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("LOAD httpfs")
    host = re.sub(r"^https?://", "", ENDPOINT)
    q = lambda v: "'" + v.replace("'", "''") + "'"  # noqa: E731  (settings come from our own .env)
    for k, v in {"s3_endpoint": host, "s3_access_key_id": KEY, "s3_secret_access_key": SECRET, "s3_region": "us-east-1", "s3_url_style": "path"}.items():
        con.execute(f"SET {k} = {q(v)}")
    con.execute(f"SET s3_use_ssl = {str(ENDPOINT.startswith('https')).lower()}")
    return con


con = _connect()


def _views() -> list[str]:
    """(Re)creates a view per gold table that exists; returns their names."""
    present = []
    for t in TABLES:
        try:
            con.execute(f"CREATE OR REPLACE VIEW {t} AS SELECT * FROM read_parquet('s3://{BUCKET}/gold/{t}/*.parquet')")
            present.append(t)
        except duckdb.Error:
            pass
    return present


def _rows(sql: str, params: list | None = None) -> list[dict]:
    with _lock:
        _views()
        cur = con.execute(sql, params or [])
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, (v.isoformat() if hasattr(v, "isoformat") else v for v in r))) for r in cur.fetchall()]


@app.get("/")
def dashboard():
    return FileResponse(os.path.join(os.path.dirname(__file__), "static", "index.html"))


@app.get("/health")
def health():
    with _lock:
        present = _views()
    return {"storage": ENDPOINT, "gold_tables": present, "ready": len(present) == len(TABLES)}


@app.get("/tables")
def tables():
    with _lock:
        present = _views()
        return {t: con.execute(f"SELECT count(*) FROM {t}").fetchone()[0] for t in present}


@app.get("/kpis/{table}")
def kpis(table: str, limit: int = 1000):
    if table not in TABLES:
        raise HTTPException(404, f"unknown table; one of {TABLES}")
    try:
        return _rows(f"SELECT * FROM {table} LIMIT ?", [max(1, min(limit, 100_000))])
    except duckdb.Error as e:
        raise HTTPException(503, f"{table} not built yet: run the DAG ({e.__class__.__name__})")


@app.get("/quality")
def quality():
    s3 = boto3.client("s3", endpoint_url=ENDPOINT, aws_access_key_id=KEY, aws_secret_access_key=SECRET,
                      config=Config(s3={"addressing_style": "path"}), region_name="us-east-1")
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key="silver/_quality/latest.json")["Body"].read())
    except s3.exceptions.NoSuchKey:
        raise HTTPException(404, "no quality report yet: run the DAG")


class Query(BaseModel):
    query: str


@app.post("/sql")
def sql(q: Query):
    text = q.query.strip().rstrip(";")
    # Read-only: one SELECT/WITH statement over the gold views.
    if not re.match(r"(?is)^(select|with)\b", text) or ";" in text:
        raise HTTPException(400, "only one SELECT or WITH statement")
    try:
        return _rows(f"SELECT * FROM ({text}) LIMIT 10000")
    except duckdb.Error as e:
        raise HTTPException(400, str(e))
