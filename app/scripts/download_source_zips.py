#!/usr/bin/env python3
"""
Download PBF (Bolsa Família) and Auxílio Brasil monthly ZIPs into a local folder.

This is a Python translation of `R/Passo1_downloadData.R` focusing on the raw ZIP download step.

Sources (Portal da Transparência):
- PBF payments:   http://www.portaltransparencia.gov.br/download-de-dados/bolsa-familia-pagamentos/YYYYMM
- Auxílio Brasil: https://portaldatransparencia.gov.br/download-de-dados/auxilio-brasil/YYYYMM
- Novo Bolsa Família (2023+): https://portaldatransparencia.gov.br/download-de-dados/novo-bolsa-familia/YYYYMM

Output filenames are chosen to be compatible with the Python ingestion regex in
`pbf_pipeline/ingest/bronze.py`:
- PBF_YYYY_MM.zip
- AUX_BR_YYYY_MM.zip   (ingestor maps AUX_BR -> origin=AUX)
- NBF_YYYY_MM.zip      (ingestor maps NBF -> origin=NBF)

Usage:
  python scripts/download_source_zips.py --out-dir data/source_zips --pbf-years 2013-2021,2023 --aux-years 2021-2023 --nbf-years 2023-2026

Notes:
- The downloader skips files that already exist.
- The Portal sometimes responds with HTML or non-zip bodies when a month does not exist.
  We detect this and delete the downloaded file.
"""

from __future__ import annotations

import argparse
import itertools
import re
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import requests
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed


PBF_BASE = "http://www.portaltransparencia.gov.br/download-de-dados/bolsa-familia-pagamentos/"
AUX_BASE = "https://portaldatransparencia.gov.br/download-de-dados/auxilio-brasil/"
NBF_BASE = "https://portaldatransparencia.gov.br/download-de-dados/novo-bolsa-familia/"

ZIP_MAGIC = b"PK\x03\x04"
ZIP_MAGIC_EMPTY = b"PK\x05\x06"


@dataclass(frozen=True)
class DownloadSpec:
    prefix: str  # "PBF" or "AUX_BR"
    base_url: str


def _parse_years(expr: str) -> list[int]:
    """
    Parse a years expression like:
      - "2013-2021" => [2013..2021]
      - "2021,2022,2023"
      - "2013-2021,2023"
    """
    years: set[int] = set()
    parts = [p.strip() for p in expr.split(",") if p.strip()]
    for part in parts:
        if "-" in part:
            a, b = part.split("-", 1)
            years.update(range(int(a), int(b) + 1))
        else:
            years.add(int(part))
    return sorted(years)


def _months() -> list[int]:
    return list(range(1, 13))


def _yyyymm(year: int, month: int) -> str:
    return f"{year}{month:02d}"


def _dest_name(prefix: str, year: int, month: int) -> str:
    return f"{prefix}_{year}_{month:02d}.zip"


def _is_zip_file(path: Path) -> bool:
    """
    Accept normal zip local-file-header magic (PK\\x03\\x04) and empty-zip EOCD (PK\\x05\\x06).
    """
    try:
        with path.open("rb") as f:
            head = f.read(4)
        return head in (ZIP_MAGIC, ZIP_MAGIC_EMPTY)
    except OSError:
        return False


def _download(url: str, dest: Path, timeout: int, retries: int, sleep_s: float) -> bool:
    """
    Returns True if a valid zip was downloaded, False otherwise.

    The Portal sometimes returns an HTML page (or JSON) with status 200 for missing months.
    We validate by checking the ZIP magic bytes and also log some response metadata.
    """
    # Ensure parent exists
    dest.parent.mkdir(parents=True, exist_ok=True)

    headers = {
        # Some servers behave better with a UA
        "User-Agent": "Mozilla/5.0 (compatible; getPBFData-downloader/1.0)",
        "Accept": "*/*",
    }

    for attempt in range(1, retries + 1):
        try:
            with requests.get(url, headers=headers, stream=True, timeout=timeout, allow_redirects=True) as r:
                r.raise_for_status()
                content_type = (r.headers.get("Content-Type") or "").lower()
                total = int(r.headers.get("Content-Length") or 0)

                tmp = dest.with_suffix(dest.suffix + ".part")
                written = 0

                with tmp.open("wb") as f:
                    bar = tqdm(
                        total=total if total > 0 else None,
                        unit="B",
                        unit_scale=True,
                        unit_divisor=1024,
                        desc=f"{dest.name}",
                        leave=False,
                    )
                    try:
                        for chunk in r.iter_content(chunk_size=1024 * 1024):
                            if not chunk:
                                continue
                            f.write(chunk)
                            written += len(chunk)
                            bar.update(len(chunk))
                    finally:
                        bar.close()

                # Quick validation: magic bytes must be ZIP.
                # If not, print a short diagnostic to help debugging.
                if written < 4 or not _is_zip_file(tmp):
                    snippet = b""
                    try:
                        with tmp.open("rb") as f:
                            snippet = f.read(200)
                    except OSError:
                        pass

                    tmp.unlink(missing_ok=True)

                    # Best-effort: avoid dumping binary; show a safe preview
                    try:
                        preview = snippet.decode("utf-8", errors="replace")
                    except Exception:
                        preview = repr(snippet)

                    print(
                        f"[NOT_ZIP] {url} -> content-type='{content_type}' bytes={written} "
                        f"head={snippet[:12]!r} preview={preview[:120]!r}",
                        file=sys.stderr,
                    )
                    return False

                tmp.replace(dest)
                return True

        except (requests.HTTPError, requests.ConnectionError, requests.Timeout) as e:
            if attempt == retries:
                print(f"[ERROR] {url} -> {e}", file=sys.stderr)
                return False
            time.sleep(sleep_s)

    return False


def iter_months(years: Iterable[int], months: Iterable[int]) -> Iterable[tuple[int, int]]:
    for y, m in itertools.product(years, months):
        yield y, m


def main() -> int:
    ap = argparse.ArgumentParser(description="Download PBF / Auxílio Brasil / Novo Bolsa Família monthly ZIPs")
    ap.add_argument(
        "--out-dir",
        type=Path,
        default=Path("data/source_zips"),
        help="Directory where ZIPs will be stored (default: data/source_zips)",
    )
    ap.add_argument(
        "--pbf-years",
        default="2013-2021",
        help="Years for PBF downloads (default: 2013-2021,2023)",
    )
    ap.add_argument(
        "--aux-years",
        default="2021-2023",
        help="Years for Auxílio Brasil downloads (default: 2021-2023; note: months typically start at 2021-11)",
    )
    ap.add_argument(
        "--nbf-years",
        default="2023-2026",
        help="Years for Novo Bolsa Família downloads (default: 2023-2026)",
    )
    ap.add_argument(
        "--months",
        default="1-12",
        help="Months to download, e.g. '1-12' or '1,2,3,10,11,12' (default: 1-12)",
    )
    ap.add_argument("--timeout", type=int, default=200, help="HTTP timeout seconds (default: 200)")
    ap.add_argument("--retries", type=int, default=3, help="Retries per URL (default: 3)")
    ap.add_argument("--sleep", type=float, default=1.5, help="Sleep between retries seconds (default: 1.5)")
    ap.add_argument(
        "--only",
        choices=["pbf", "aux", "nbf", "both"],
        default="both",
        help="Which datasets to download (default: both)",
    )
    ap.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Number of parallel download workers (default: 4). Each worker downloads one ZIP at a time.",
    )

    args = ap.parse_args()

    # months parse
    months_expr = args.months.strip()
    months: list[int] = []
    if re.fullmatch(r"\d+\-\d+", months_expr):
        a, b = months_expr.split("-", 1)
        months = list(range(int(a), int(b) + 1))
    else:
        months = [int(x) for x in months_expr.split(",") if x.strip()]

    specs: list[DownloadSpec] = []
    if args.only in ("pbf", "both"):
        specs.append(DownloadSpec(prefix="PBF", base_url=PBF_BASE))
    if args.only in ("aux", "both"):
        specs.append(DownloadSpec(prefix="AUX_BR", base_url=AUX_BASE))
    if args.only in ("nbf", "both"):
        specs.append(DownloadSpec(prefix="NBF", base_url=NBF_BASE))

    pbf_years = _parse_years(args.pbf_years)
    aux_years = _parse_years(args.aux_years)
    nbf_years = _parse_years(args.nbf_years)

    # Build tasks first so we can run them in parallel.
    tasks: list[tuple[str, str, Path]] = []  # (prefix, url, dest)
    total = 0
    ok = 0
    skipped = 0
    missing = 0

    for spec in specs:
        if spec.prefix == "PBF":
            years = pbf_years
        elif spec.prefix == "AUX_BR":
            years = aux_years
        else:
            years = nbf_years

        for year, month in iter_months(years, months):
            total += 1
            yyyymm = _yyyymm(year, month)
            url = f"{spec.base_url}{yyyymm}"
            dest = args.out_dir / _dest_name(spec.prefix, year, month)

            if dest.exists() and dest.stat().st_size > 0 and _is_zip_file(dest):
                skipped += 1
                continue

            tasks.append((spec.prefix, url, dest))

    def _run_one(prefix: str, url: str, dest: Path) -> tuple[bool, str, str, Path]:
        downloaded = _download(
            url=url,
            dest=dest,
            timeout=args.timeout,
            retries=args.retries,
            sleep_s=args.sleep,
        )
        return downloaded, prefix, url, dest

    # Run downloads in parallel. NOTE: tqdm progress bars from multiple threads will interleave;
    # each bar is still visible but not perfectly pretty.
    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futures = [ex.submit(_run_one, prefix, url, dest) for prefix, url, dest in tasks]
        for fut in as_completed(futures):
            downloaded, prefix, url, dest = fut.result()
            yyyymm = dest.stem.split("_", 2)[1].replace("-", "")
            if downloaded:
                ok += 1
                print(f"[OK] {prefix} -> {dest}")
            else:
                missing += 1
                dest.unlink(missing_ok=True)
                print(f"[MISS] {prefix} (no zip at {url})")

    print(
        f"Done. total={total} ok={ok} skipped={skipped} missing={missing}. "
        f"Output dir: {args.out_dir.resolve()}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
