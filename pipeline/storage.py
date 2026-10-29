"""Small object-storage helpers (MinIO / any S3) used outside Spark."""
from __future__ import annotations

import json

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

from pipeline.config import Settings


def client(settings: Settings):
    return boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint,
        aws_access_key_id=settings.s3_access_key,
        aws_secret_access_key=settings.s3_secret_key,
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
        region_name="us-east-1",
    )


def put_bytes(s3, bucket: str, key: str, body: bytes, content_type: str = "text/csv") -> None:
    s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType=content_type)


def put_json(s3, bucket: str, key: str, doc: dict) -> None:
    put_bytes(s3, bucket, key, json.dumps(doc, indent=2, default=str).encode(), "application/json")


def get_json(s3, bucket: str, key: str) -> dict | None:
    try:
        return json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())
    except ClientError as e:
        if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
            return None
        raise


def delete_prefix(s3, bucket: str, prefix: str) -> int:
    """Removes every object under `prefix` (a month re-ingested from scratch). Returns how many."""
    n = 0
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        keys = [{"Key": o["Key"]} for o in page.get("Contents", [])]
        if keys:
            s3.delete_objects(Bucket=bucket, Delete={"Objects": keys})
            n += len(keys)
    return n
