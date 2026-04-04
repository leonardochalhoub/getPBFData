from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path

import argparse

from shared.spark import SparkConfig, SparkFactory
from bronze.bronze import BronzePaths, ZipPaymentsBronzeIngestor


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="PBF/AuxBrasil/NBF pipeline (Delta Lake)")
    sub = p.add_subparsers(dest="cmd", required=True)

    b = sub.add_parser("ingest-bronze", help="Ingest raw ZIPs into Bronze Delta")
    b.add_argument(
        "--source-zips-dir",
        required=True,
        type=Path,
        help="Directory containing monthly ZIP files",
    )
    b.add_argument(
        "--lakehouse-root",
        required=True,
        type=Path,
        help="Root path of the lakehouse folder (will use lakehouse/bronze)",
    )
    b.add_argument("--master", default=None, help="Spark master, e.g. local[*]")
    b.add_argument("--shuffle-partitions", default=200, type=int)
    b.add_argument("--batch-size", default=12, type=int, help="How many (year,month) groups to ingest per Spark batch")
    b.add_argument(
        "--only-origin",
        default=None,
        help="Comma-separated list of origins to ingest (e.g. 'NBF' or 'PBF,AUX'). Default: all",
    )
    b.add_argument(
        "--min-competencia",
        default=None,
        help="Minimum competencia (inclusive) in YYYYMM format. Example: 202409",
    )

    return p


def cmd_ingest_bronze(args: argparse.Namespace) -> None:
    spark = SparkFactory.create(
        SparkConfig(
            app_name="pbf-ingest-bronze",
            master=args.master,
            shuffle_partitions=args.shuffle_partitions,
        )
    )

    paths = BronzePaths.for_lakehouse(source_zips_dir=args.source_zips_dir, lakehouse_root=args.lakehouse_root)

    origin_allow = None
    if args.only_origin:
        origin_allow = {x.strip().upper() for x in args.only_origin.split(",") if x.strip()}

    min_year_month = None
    if args.min_competencia:
        s = str(args.min_competencia).strip()
        if len(s) != 6 or not s.isdigit():
            raise SystemExit("--min-competencia must be in YYYYMM format (e.g. 202409)")
        min_year_month = (int(s[:4]), int(s[4:]))

    ZipPaymentsBronzeIngestor(spark=spark, paths=paths).ingest_all(
        batch_size=args.batch_size,
        origin_allow=origin_allow,
        min_year_month=min_year_month,
    )


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.cmd == "ingest-bronze":
        cmd_ingest_bronze(args)
    else:
        raise SystemExit(f"Unknown cmd: {args.cmd}")


if __name__ == "__main__":
    main()
