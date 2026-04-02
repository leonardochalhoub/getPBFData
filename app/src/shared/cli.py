from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path

import argparse

from pbf_pipeline.spark import SparkConfig, SparkFactory
from pbf_pipeline.ingest.bronze import BronzePaths, ZipPaymentsBronzeIngestor


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
        "--bronze-root",
        required=True,
        type=Path,
        help="Root path where bronze delta tables will be stored",
    )
    b.add_argument("--master", default=None, help="Spark master, e.g. local[*]")
    b.add_argument("--shuffle-partitions", default=200, type=int)

    return p


def cmd_ingest_bronze(args: argparse.Namespace) -> None:
    spark = SparkFactory.create(
        SparkConfig(
            app_name="pbf-ingest-bronze",
            master=args.master,
            shuffle_partitions=args.shuffle_partitions,
        )
    )

    paths = BronzePaths(source_zips_dir=args.source_zips_dir, bronze_root=args.bronze_root)
    ZipPaymentsBronzeIngestor(spark=spark, paths=paths).ingest_all()


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.cmd == "ingest-bronze":
        cmd_ingest_bronze(args)
    else:
        raise SystemExit(f"Unknown cmd: {args.cmd}")


if __name__ == "__main__":
    main()
