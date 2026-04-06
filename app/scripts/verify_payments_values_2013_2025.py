"""
Verify that payment totals are computed consistently from Bronze -> Silver -> Gold.

Goal
----
Detect whether the same *value paid* column is being used across the entire time range
(2013..2025) and whether there are schema/column changes around 2022+ that could explain
large shifts.

What this script checks
-----------------------
1) Bronze schema over time:
   - For each (ano, mes, origin) partition in the Bronze Delta table, it inspects:
     - whether a "valor_parcela" column exists
     - whether it is parseable to Decimal(38,2)
     - basic stats (sum, count, nulls, min/max)
   - It also reports the distinct set of column names observed per year (approx: using a sample).

2) Silver totals:
   - Recomputes Silver `total_ano_mes_estados` directly from Bronze using the project logic:
       - apply Nov/2021 origin rule
       - parse `valor_parcela` to decimal
       - group by (mes_competencia, uf) and sum(valor_parcela)
   - Compares the recomputed result with the materialized Silver table (if present).

3) Gold yearly totals:
   - Aggregates Silver to yearly totals and compares with the Gold table
     `gold/pbf_estados_df_geo` (if present), using `valor_nominal` as the yearly sum.

Usage
-----
PYTHONPATH=. python app/scripts/verify_payments_values_2013_2025.py \\
  --lakehouse-root lakehouse \\
  --start-year 2013 --end-year 2025

Notes
-----
- Designed to run locally with Spark + Delta (same stack used in the pipeline).
- It is safe: read-only. It does not overwrite any tables.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional

from pyspark.sql import DataFrame, functions as F, types as T

from app.src.silver.common import (
    LakehousePaths,
    apply_nov_2021_origin_rule,
    build_delta_spark,
    parse_valor_parcela_decimal38,
    with_ano_mes_from_mes_competencia,
)


DEC = T.DecimalType(38, 2)


@dataclass(frozen=True)
class Range:
    start_year: int
    end_year: int

    def years(self) -> list[int]:
        return list(range(self.start_year, self.end_year + 1))


def _exists_delta(spark, path: str) -> bool:
    try:
        spark.read.format("delta").load(path).limit(1).collect()
        return True
    except Exception:
        return False


def _bronze_partition_stats(df_bronze: DataFrame, *, year: int, month: int, origin: Optional[str]) -> DataFrame:
    """
    Compute basic stats for a single Bronze partition slice.

    Returns a single-row DataFrame with:
      ano, mes, origin, rows, has_valor_parcela, nulls_valor_parcela, sum_valor_parcela, min/max
    """
    df = df_bronze.where((F.col("ano") == F.lit(year)) & (F.col("mes") == F.lit(month)))
    if origin is not None:
        df = df.where(F.col("origin") == F.lit(origin))

    has_col = "valor_parcela" in df_bronze.columns

    if not has_col:
        return (
            df.select("ano", "mes", "origin")
            .limit(1)
            .withColumn("rows", F.lit(None).cast("long"))
            .withColumn("has_valor_parcela", F.lit(False))
            .withColumn("nulls_valor_parcela", F.lit(None).cast("long"))
            .withColumn("sum_valor_parcela_dec", F.lit(None).cast(DEC))
            .withColumn("min_valor_parcela_dec", F.lit(None).cast(DEC))
            .withColumn("max_valor_parcela_dec", F.lit(None).cast(DEC))
        )

    df2 = parse_valor_parcela_decimal38(df, input_col="valor_parcela", output_col="valor_parcela_dec")

    return (
        df2.agg(
            F.first(F.col("ano")).alias("ano"),
            F.first(F.col("mes")).alias("mes"),
            F.first(F.col("origin")).alias("origin"),
            F.count(F.lit(1)).cast("long").alias("rows"),
            F.lit(True).alias("has_valor_parcela"),
            F.sum(F.when(F.col("valor_parcela").isNull(), F.lit(1)).otherwise(F.lit(0))).cast("long").alias(
                "nulls_valor_parcela"
            ),
            F.sum(F.col("valor_parcela_dec")).alias("sum_valor_parcela_dec"),
            F.min(F.col("valor_parcela_dec")).alias("min_valor_parcela_dec"),
            F.max(F.col("valor_parcela_dec")).alias("max_valor_parcela_dec"),
        )
    )


def _recompute_silver_from_bronze(df_bronze: DataFrame) -> DataFrame:
    """
    Recompute the Silver monthly totals by UF from Bronze using project logic.
    """
    df = apply_nov_2021_origin_rule(df_bronze)
    df = parse_valor_parcela_decimal38(df)

    benef_id = F.regexp_replace(F.trim(F.col("nis_favorecido")), r"\\D", "")
    df = df.withColumn("_benef_id", benef_id).where(F.length(F.col("_benef_id")) > 0)

    out = (
        df.groupBy("mes_competencia", "uf")
        .agg(
            F.countDistinct("_benef_id").alias("n"),
            F.sum(F.col("valor_parcela_dec")).alias("total_estado"),
        )
        .transform(with_ano_mes_from_mes_competencia)
        .select("Ano", "Mes", "uf", "mes_competencia", "n", "total_estado")
    )
    return out


def _print_header(title: str) -> None:
    print()
    print("=" * len(title))
    print(title)
    print("=" * len(title))


def main(argv: Optional[Iterable[str]] = None) -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--lakehouse-root", type=str, default="lakehouse")
    ap.add_argument("--start-year", type=int, default=2013)
    ap.add_argument("--end-year", type=int, default=2025)
    ap.add_argument("--sample-fraction", type=float, default=0.001, help="Sampling fraction for bronze column profiling")
    args = ap.parse_args(list(argv) if argv is not None else None)

    yr = Range(args.start_year, args.end_year)

    spark = build_delta_spark("verify-payments-values-2013-2025")
    paths = LakehousePaths(lakehouse_root=Path(args.lakehouse_root))

    bronze_path = paths.bronze_payments_path
    silver_path = str(paths.silver_root / "total_ano_mes_estados")
    gold_path = str(paths.lakehouse_root / "gold" / "pbf_estados_df_geo")

    _print_header("Loading Bronze payments")
    df_bronze = spark.read.format("delta").load(bronze_path)

    # Limit to requested range (bronze has ano/mes int columns)
    df_bronze = df_bronze.where((F.col("ano") >= F.lit(yr.start_year)) & (F.col("ano") <= F.lit(yr.end_year)))

    # 1) Bronze: basic schema presence checks
    _print_header("Bronze: schema presence and value stats by year/month/origin (high level)")
    print("Bronze columns include valor_parcela:", "valor_parcela" in df_bronze.columns)
    print("Bronze columns (first 40):", df_bronze.columns[:40])

    # Partition list (small): distinct year/month/origin observed
    parts = df_bronze.select("ano", "mes", "origin").distinct()

    # Per-year counts (sanity)
    by_year = df_bronze.groupBy("ano").agg(F.count(F.lit(1)).alias("rows")).orderBy("ano")
    by_year.show(200, truncate=False)

    # Focused anomaly check: 2020..2025 monthly sums by origin
    _print_header("Bronze: monthly sums of valor_parcela (parsed) by origin (2020..2025) to spot 2022+ shifts")
    if "valor_parcela" in df_bronze.columns:
        df_b = parse_valor_parcela_decimal38(df_bronze)
        (
            df_b.where(F.col("ano") >= F.lit(2020))
            .groupBy("ano", "mes", "origin")
            .agg(
                F.count(F.lit(1)).alias("rows"),
                F.sum(F.col("valor_parcela_dec")).alias("sum_valor_parcela_dec"),
            )
            .orderBy("ano", "mes", "origin")
            .show(500, truncate=False)
        )
    else:
        print("Cannot compute sums: bronze is missing valor_parcela column entirely.")

    # 1b) Column profiling (sample) to detect changing header names across years
    _print_header("Bronze: sampled per-year column non-null rates for candidate value columns")
    candidate_value_cols = [
        "valor_parcela",
        "valor_beneficio",
        "valor_beneficio_saque",
        "valor_saque",
        "valor_pago",
        "valor",
        "valor_total",
    ]
    present_candidates = [c for c in candidate_value_cols if c in df_bronze.columns]
    print("Present candidate columns:", present_candidates)

    if present_candidates:
        # sample for performance
        df_s = df_bronze.sample(withReplacement=False, fraction=float(args.sample_fraction), seed=42)
        exprs = []
        for c in present_candidates:
            exprs.append((F.avg(F.when(F.col(c).isNotNull(), F.lit(1.0)).otherwise(F.lit(0.0)))).alias(f"nonnull_{c}"))
        df_s.groupBy("ano").agg(*exprs).orderBy("ano").show(200, truncate=False)

    # 2) Silver comparison
    _print_header("Silver: recompute from Bronze and compare to materialized Silver (if exists)")
    df_re = _recompute_silver_from_bronze(df_bronze).cache()
    print("Recomputed silver rows:", df_re.count())

    if _exists_delta(spark, silver_path):
        df_silver = spark.read.format("delta").load(silver_path)
        df_silver = df_silver.where((F.col("Ano") >= F.lit(yr.start_year)) & (F.col("Ano") <= F.lit(yr.end_year)))

        # Compare aggregates by (Ano, Mes, uf)
        a = (
            df_re.groupBy("Ano", "Mes", "uf")
            .agg(F.sum("total_estado").alias("re_total_estado"), F.max("n").alias("re_n"))
            .withColumnRenamed("uf", "uf_key")
        )
        b = (
            df_silver.groupBy("Ano", "Mes", "uf")
            .agg(F.sum("total_estado").alias("sv_total_estado"), F.max("n").alias("sv_n"))
            .withColumnRenamed("uf", "uf_key")
        )

        df_cmp = (
            a.join(b, on=["Ano", "Mes", "uf_key"], how="outer")
            .withColumn("diff_total_estado", F.col("re_total_estado") - F.col("sv_total_estado"))
            .withColumn("diff_n", F.col("re_n") - F.col("sv_n"))
            .orderBy("Ano", "Mes", "uf_key")
        )

        # Show only mismatches above rounding tolerance
        df_mis = df_cmp.where(
            (F.abs(F.col("diff_total_estado")) > F.lit(0.01)) | (F.abs(F.col("diff_n")) > F.lit(0))
        )
        print("Mismatching (Ano,Mes,UF) rows:", df_mis.count())
        df_mis.show(200, truncate=False)
    else:
        print(f"Silver table not found at {silver_path} (skipping materialized Silver comparison).")

    # 3) Gold yearly totals comparison
    _print_header("Gold: yearly totals comparison (Silver->Year vs Gold.valor_nominal) (if exists)")
    df_year_from_re = (
        df_re.groupBy("Ano", "uf")
        .agg((F.sum("total_estado") / F.lit(1e9)).alias("valor_nominal_re"), F.max("n").alias("n_benef_re"))
        .orderBy("Ano", "uf")
    )

    if _exists_delta(spark, gold_path):
        df_gold = spark.read.format("delta").load(gold_path)

        # keep only numeric Ano rows
        df_gold_y = (
            df_gold.where(F.col("Ano").cast("string").rlike("^\\d+$"))
            .withColumn("Ano", F.col("Ano").cast("int"))
            .select("Ano", "uf", "valor_nominal", "n_benef")
        )

        df_gcmp = (
            df_year_from_re.join(df_gold_y, on=["Ano", "uf"], how="inner")
            .withColumn("diff_valor_nominal_bi", F.col("valor_nominal_re") - F.col("valor_nominal"))
            .withColumn("diff_n_benef", F.col("n_benef_re") - F.col("n_benef"))
            .orderBy("Ano", "uf")
        )

        # show biggest diffs
        df_gcmp.orderBy(F.abs(F.col("diff_valor_nominal_bi")).desc()).show(50, truncate=False)

        # highlight 2022+ only
        _print_header("Gold diffs for 2022+")
        df_gcmp.where(F.col("Ano") >= F.lit(2022)).orderBy(F.abs(F.col("diff_valor_nominal_bi")).desc()).show(
            200, truncate=False
        )
    else:
        print(f"Gold table not found at {gold_path} (skipping gold comparison).")

    spark.stop()


if __name__ == "__main__":
    main()
