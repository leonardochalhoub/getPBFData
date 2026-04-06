"""
Debug script: investigate why yearly distinct beneficiaries (n_ano / n_benef) are close to population.
Compares Silver vs Gold (should match), and compares to population to quantify the issue.
Also prints top UF-years by penetration (n_benef / population).
"""
from __future__ import annotations
import argparse
from pathlib import Path
from pyspark.sql import functions as F
from app.src.silver.common import LakehousePaths, build_delta_spark
from app.src.gold.pbf_estados_df_geo import GoldPaths


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--lakehouse-root", type=str, default="lakehouse")
    ap.add_argument("--year", type=int, default=None, help="Optional filter year (e.g. 2023)")
    ap.add_argument("--limit", type=int, default=50)
    args = ap.parse_args()

    lake = Path(args.lakehouse_root)
    spark = build_delta_spark("debug-n-ano-vs-population")

    sp = LakehousePaths(lakehouse_root=lake)
    gp = GoldPaths(lakehouse_root=lake)

    # SILVER: yearly distinct beneficiaries (n_ano) per UF-year (dedup across months)
    df_silver = (
        spark.read.format("delta")
        .load(str(sp.silver_root / "total_ano_mes_estados"))
        .select("Ano", "uf", "n_ano")
        .distinct()
        .withColumn("Ano", F.col("Ano").cast("int"))
        .withColumn("n_ano", F.col("n_ano").cast("long"))
    )

    # GOLD: yearly distinct beneficiaries (n_benef) per UF-year (numeric years only).
    # NOTE: gold stores "Ano" as string; numeric years are strings like "2019".
    df_gold = (
        spark.read.format("delta")
        .load(gp.gold_pbf_estados_df_geo_path)
        .where(F.col("Ano").rlike("^[0-9]+$"))
        .select(
            F.col("Ano").cast("int").alias("Ano"),
            "uf",
            F.col("n_benef").cast("long").alias("n_benef"),
            F.col("populacao").cast("long").alias("populacao"),
        )
    )

    if args.year is not None:
        df_silver = df_silver.where(F.col("Ano") == F.lit(int(args.year)))
        df_gold = df_gold.where(F.col("Ano") == F.lit(int(args.year)))

    print()
    print("=======================")
    print("Key sanity checks")
    print("=======================")
    print("silver distinct (Ano,uf):", df_silver.select("Ano", "uf").distinct().count())
    print("gold   distinct (Ano,uf):", df_gold.select("Ano", "uf").distinct().count())

    print()
    print("Silver Ano range:")
    df_silver.agg(F.min("Ano").alias("min"), F.max("Ano").alias("max")).show(truncate=False)

    print("Gold Ano range:")
    df_gold.agg(F.min("Ano").alias("min"), F.max("Ano").alias("max")).show(truncate=False)

    print()
    print("Sample Silver keys:")
    df_silver.select("Ano", "uf").orderBy("Ano", "uf").show(10, truncate=False)

    print("Sample Gold keys:")
    df_gold.select("Ano", "uf").orderBy("Ano", "uf").show(10, truncate=False)

    # 1) Silver vs Gold consistency on beneficiaries
    df_cmp = (
        df_silver.alias("s")
        .join(df_gold.alias("g"), on=["Ano", "uf"], how="inner")
        .select(
            "Ano",
            "uf",
            F.col("s.n_ano").alias("silver_n_ano"),
            F.col("g.n_benef").alias("gold_n_benef"),
            (F.col("g.n_benef") - F.col("s.n_ano")).alias("diff"),
            "populacao",
        )
    )

    n_diffs = df_cmp.where(F.col("diff") != 0).count()
    print()
    print("=====================================")
    print("Silver vs Gold (beneficiaries) check")
    print("=====================================")
    print("rows compared:", df_cmp.count())
    print("rows with diff != 0:", n_diffs)
    if n_diffs:
        df_cmp.where(F.col("diff") != 0).orderBy(F.abs(F.col("diff")).desc()).show(args.limit, truncate=False)

    # 2) Penetration vs population
    df_pen = (
        df_cmp.withColumn("penetration", F.col("gold_n_benef") / F.col("populacao"))
        .withColumn("penetration_pct", F.round(F.col("penetration") * F.lit(100.0), 2))
    )

    print()
    print("============================================")
    print("Top UF-years by n_benef / population (Gold)")
    print("============================================")
    df_pen.orderBy(F.col("penetration").desc()).show(args.limit, truncate=False)

    print()
    print("==============================================")
    print("Summary penetration by year (avg/min/max over UFs)")
    print("==============================================")
    (
        df_pen.groupBy("Ano")
        .agg(
            F.round(F.avg("penetration") * 100.0, 2).alias("avg_pct"),
            F.round(F.min("penetration") * 100.0, 2).alias("min_pct"),
            F.round(F.max("penetration") * 100.0, 2).alias("max_pct"),
        )
        .orderBy("Ano")
        .show(200, truncate=False)
    )

    spark.stop()


if __name__ == "__main__":
    main()
