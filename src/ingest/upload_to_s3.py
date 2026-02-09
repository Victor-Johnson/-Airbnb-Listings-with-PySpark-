
from __future__ import annotations

import os
from pathlib import Path
from typing import Iterable, Tuple

import boto3
from botocore.exceptions import ClientError


def iter_files(root: Path) -> Iterable[Path]:
    for p in root.rglob("*"):
        if p.is_file():
            yield p


def upload_folder_to_s3(
    local_root: Path,
    bucket: str,
    s3_prefix: str,
) -> Tuple[int, int]:
    """
    Uploads all files under local_root to s3://{bucket}/{s3_prefix}/... preserving relative paths.
    Returns (files_uploaded, bytes_uploaded).
    """
    s3 = boto3.client("s3")

    if not local_root.exists() or not local_root.is_dir():
        raise FileNotFoundError(f"Local folder not found: {local_root}")

    # Normalize prefix
    s3_prefix = s3_prefix.strip("/")
    files_uploaded = 0
    bytes_uploaded = 0

    for file_path in iter_files(local_root):
        rel = file_path.relative_to(local_root).as_posix()
        key = f"{s3_prefix}/{rel}" if s3_prefix else rel

        try:
            s3.upload_file(str(file_path), bucket, key)
            files_uploaded += 1
            bytes_uploaded += file_path.stat().st_size
            print(f" Uploaded: s3://{bucket}/{key}")
        except ClientError as e:
            raise RuntimeError(f"Upload failed for {file_path} -> s3://{bucket}/{key}\n{e}") from e

    return files_uploaded, bytes_uploaded


if __name__ == "__main__":
    # You should have AWS creds configured via:
    # - aws configure (recommended), OR
    # - environment variables, OR
    # - an EC2 role (if running on AWS)
    bucket = os.getenv("AIRBNB_S3_BUCKET", "")
    if not bucket:
        raise ValueError("Set AIRBNB_S3_BUCKET env var (e.g. export AIRBNB_S3_BUCKET='victor-airbnb-datalake')")

    city = os.getenv("AIRBNB_CITY", "london")
    snapshot_date = os.getenv("AIRBNB_SNAPSHOT_DATE", "2025-09-14")

    local_root = Path(os.getenv("AIRBNB_LOCAL_ROOT", f"data/raw/source=insideairbnb/city={city}/snapshot_date={snapshot_date}"))

    # This prefix mirrors your lake layout in S3
    s3_prefix = os.getenv("AIRBNB_S3_PREFIX", f"raw/source=insideairbnb/city={city}/snapshot_date={snapshot_date}")

    print(f" Uploading folder: {local_root}")
    print(f"  Target: s3://{bucket}/{s3_prefix}/")

    files_count, total_bytes = upload_folder_to_s3(local_root, bucket, s3_prefix)

    mb = total_bytes / (1024 * 1024)
    print(f"\n✅ Done. Uploaded {files_count} files ({mb:.2f} MB total).")
