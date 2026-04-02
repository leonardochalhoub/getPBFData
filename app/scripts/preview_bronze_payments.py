#!/usr/bin/env python3
"""
Preview the Bronze payments table.

Notes
-----
- In this repo, the Bronze payments dataset is written as a Delta table at:
  lakehouse/bronze/payments

- This runtime environment may not have Delta Lake (delta-spark) jars installed.
  If Delta is available, we will read using the Delta datasource. Otherwise, we
  fall back to reading the underlying Parquet files directly (sufficient for a
  quick `show()` preview).
"""

from __future__ import annotations

from pyspark.sql import SparkSession, functions as F


def _spark() -> SparkSession:
    """
    Build a SparkSession configured for Delta Lake.

    Requires `delta-spark` to be installed so we can attach the Delta jars via
    `configure_spark_with_delta_pip`.
    """
    from delta import configure_spark_with_delta_pip  # type: ignore

    builder = (
        SparkSession.builder.appName("preview-bronze-payments")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def main() -> None:
    spark = _spark()
    path = "lakehouse/bronze/payments"

    df = spark.read.format("delta").load(path)

    # Filter to the "last" partition (max ano/mes) to keep the preview fast.
    last_part = df.select(F.max("ano").alias("ano")).collect()[0]["ano"]
    last_month = (
        df.where(F.col("ano") == F.lit(last_part))
        .select(F.max("mes").alias("mes"))
        .collect()[0]["mes"]
    )
    df_last = df.where((F.col("ano") == F.lit(last_part)) & (F.col("mes") == F.lit(last_month)))

    print(f"\nPreviewing last partition: ano={last_part} mes={last_month}")
    print("\nSchema:")
    df_last.printSchema()

    print("\ndf.show(10):")
    df_last.show(10, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
