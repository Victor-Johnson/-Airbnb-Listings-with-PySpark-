from __future__ import annotations

import gzip
import os
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Dict, Tuple

import requests


@dataclass
class IngestConfig:
    city: str = "london"
    snapshot_date: str = "2025-09-14"  # yyyy-mm-dd
    base_dir: str = "data/raw"
    timeout_sec: int = 180


def _safe_filename(name: str) -> str:
    return name.replace("/", "_").replace(":", "_")


def download_file(url: str, out_path: Path, timeout_sec: int = 60) -> Path:
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with requests.get(url, stream=True, timeout=timeout_sec) as r:
        r.raise_for_status()
        with open(out_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)

    size_mb = out_path.stat().st_size / (1024 * 1024)
    print(f" Downloaded: {out_path} ({size_mb:.2f} MB)")
    return out_path


def quick_gzip_check(gz_path: Path, max_lines: int = 5) -> None:
    print(f"Quick check (first {max_lines} lines): {gz_path.name}")
    with gzip.open(gz_path, "rt", encoding="utf-8", errors="replace") as f:
        for i in range(max_lines):
            line = f.readline()
            if not line:
                break
            print(line.strip()[:200])


def build_insideairbnb_urls(city: str, snapshot_date: str) -> Dict[str, str]:
    # Inside Airbnb pattern for London
    base = f"https://data.insideairbnb.com/united-kingdom/england/{city}/{snapshot_date}/data"
    return {
        "listings": f"{base}/listings.csv.gz",
        "reviews": f"{base}/reviews.csv.gz",
    }


def run(cfg: IngestConfig) -> Tuple[Path, Path]:
    urls = build_insideairbnb_urls(cfg.city, cfg.snapshot_date)

    out_dir = Path(cfg.base_dir) / f"source=insideairbnb" / f"city={cfg.city}" / f"snapshot_date={cfg.snapshot_date}"
    listings_path = out_dir / "listings.csv.gz"
    reviews_path = out_dir / "reviews.csv.gz"

    print(f" Saving raw snapshot to: {out_dir}")

    lp = download_file(urls["listings"], listings_path, cfg.timeout_sec)
    rp = download_file(urls["reviews"], reviews_path, cfg.timeout_sec)


    quick_gzip_check(lp)
    quick_gzip_check(rp)

    print(" Ingestion of data complete.")
    return lp, rp


if __name__ == "__main__":
    cfg = IngestConfig(
        city=os.getenv("AIRBNB_CITY", "london"),
        snapshot_date=os.getenv("AIRBNB_SNAPSHOT_DATE", "2025-09-14"),
        base_dir=os.getenv("AIRBNB_BASE_DIR", "data/raw"),
    )
    run(cfg)
