"""
Verify ingestion idempotency heuristics for the Bronze payments table.

This script loads ``lakehouse/bronze/payments`` and performs a quick duplicate heuristic
check on a known partition by hashing all columns (except ``ingest_ts``).

Run:
  ``PYTHONPATH=. python app/scripts/verify_idempotency.py``
"""

from __future__ import annotations

from pyspark.sql import functions as F

from shared.spark import SparkConfig, SparkFactory


def main() -> None:
    """
    Print a small partition sample and test for duplicates in a representative partition.

    The duplicate check uses:
      - filter to (origin="PBF", ano=2013, mes=1)
      - compute sha256 over all columns (casting to string), excluding ``ingest_ts``
      - compare total rows vs distinct hashes

    Notes:
        This is a heuristic: it detects exact duplicates, not near-duplicates.

    Raises:
        Any Spark errors encountered during read/aggregation.
    """
    spark = SparkFactory.create(SparkConfig(app_name="verify-bronze", master="local[*]", shuffle_partitions=24))
    df = spark.read.format("delta").load("lakehouse/bronze/payments")

    print("Partitions sample (origin, ano, mes, count):")
    for r in (
        df.groupBy("origin", "ano", "mes")
        .count()
        .orderBy("origin", "ano", "mes")
        .limit(20)
        .collect()
    ):
        print(r)

    cols = [c for c in df.columns if c != "ingest_ts"]
    part = df.where((F.col("origin") == "PBF") & (F.col("ano") == 2013) & (F.col("mes") == 1))
    part2 = part.withColumn(
        "row_hash",
        F.sha2(F.concat_ws("||", *[F.col(c).cast("string") for c in cols]), 256),
    )
    total = part2.count()
    distinct_hashes = part2.select("row_hash").distinct().count()
    print("PBF 2013-01 total:", total)
    print("PBF 2013-01 distinct hashes:", distinct_hashes)
    print("No duplicates by this heuristic:", total == distinct_hashes)

    spark.stop()


if __name__ == "__main__":
    main()
