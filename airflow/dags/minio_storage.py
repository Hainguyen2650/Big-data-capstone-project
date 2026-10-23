from __future__ import annotations

import os
from pathlib import Path
from typing import Any

def upload_record_files(records: list[dict[str, Any]]) -> int:
 
    endpoint = os.getenv("MINIO_ENDPOINT", "").strip()
    if not endpoint:
        return 0

    import boto3
    from botocore.client import Config

    bucket = os.getenv("MINIO_BUCKET", "financial-disclosures").strip()
    if not bucket:
        raise ValueError("MINIO_BUCKET must not be empty")
    if "://" not in endpoint:
        scheme = "https" if os.getenv("MINIO_USE_SSL", "false").lower() in {"1", "true", "yes"} else "http"
        endpoint = f"{scheme}://{endpoint}"
    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        region_name=os.getenv("MINIO_REGION", "us-east-1"),
        config=Config(signature_version="s3v4"),
    )
    try:
        client.head_bucket(Bucket=bucket)
    except Exception as exc:
        code = str(getattr(exc, "response", {}).get("Error", {}).get("Code", ""))
        if code not in {"404", "NoSuchBucket", "NotFound"}:
            raise
        client.create_bucket(Bucket=bucket)

    uploaded = 0
    for record in records:
        for item in record.get("local_files", []):
            local_path = item.get("path")
            if not local_path:
                continue
            path = Path(local_path)
            if not path.is_file():
                item["upload_error"] = f"Local file not found: {local_path}"
                continue
            key = "/".join((record["source"], record.get("ticker") or "unknown",
                            record["disclosure_id"], path.name))
            extra_args = {}
            if item.get("content_type"):
                extra_args["ContentType"] = item["content_type"]
            client.upload_file(str(path), bucket, key, ExtraArgs=extra_args)
            item["bucket"] = bucket
            item["object_key"] = key
            item["object_uri"] = f"s3://{bucket}/{key}"
            uploaded += 1
    return uploaded
