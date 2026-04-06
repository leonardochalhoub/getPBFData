"""
Quick check: recompute Gold (UF×Ano) from materialized Silver and compare to materialized Gold.

Purpose
-------
Fast verification of the `n_benef` logic without re-reading Bronze (which is slow).

It:
1) Reads Silver `silver/total_ano_mes_estados`
2) Runs `build_pbf_estados_df_geo(...)` (Gold transform) using the current code
3) Compares (Ano, uf) against materialized Gold `gold/pbf_estados_df_geo`
4) Prints the biggest diffs for `n_benef` and `valor_nominal`

Usage
-----
PYTHONPATH=. python app/scripts/quick_check_gold_n_benef_from_silver.py --lakehouse-root lakehouse

Notes
-----
- Read-only (does not overwrite Gold).
- Filters out the \"Agregado ...\" rows (keeps only numeric years).
"""

from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import functions as F

from app.src.gold.pbf_estados_df_geo import (
    GoldPaths,
    _build_year_december_deflators_to_2021,
    _load_states_geo_stub,
    _read_populacao_estados_from_silver,
    build_pbf_estados_df_geo,
)
from app.src.silver.common import LakehousePaths, build_delta_spark


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--lakehouse-root", type=str, default="lakehouse")
    ap.add_argument("--limit", type=int, default=50)
    args = ap.parse_args()

    lake = Path(args.lakehouse_root)

    spark = build_delta_spark("quick-check-gold-n-benef-from-silver")
    sp = LakehousePaths(lakehouse_root=lake)
    gp = GoldPaths(lakehouse_root=lake)

    # Read Silver (fast; no Bronze)
    silver_path = str(sp.silver_root / "total_ano_mes_estados")
    df_total = spark.read.format("delta").load(silver_path)

    bounds = df_total.select(F.min("Ano").alias("min"), F.max("Ano").alias("max")).collect()[0]
    min_ano = int(bounds["min"])
    max_ano = int(bounds["max"])

    # Auxiliary inputs for Gold transform
    df_pop = _read_populacao_estados_from_silver(
        spark,
        silver_populacao_uf_ano_path=gp.silver_populacao_uf_ano_path,
        start_year=min_ano,
        end_year=max_ano,
    )
    df_defl = _build_year_december_deflators_to_2021(spark, end_year=max_ano)
    df_geo = _load_states_geo_stub(spark)

    # Recompute Gold from Silver using current code
    df_gold_re = (
        build_pbf_estados_df_geo(
            df_total,
            df_populacao_estados=df_pop,
            df_states_geo=df_geo,
            df_deflators_to_2021=df_defl,
        )
        .where(F.col("Ano").cast("string").rlike(r"^\\d+$"))
        .withColumn("Ano", F.col("Ano").cast("int"))
        .select("Ano", "uf", "n_benef", "valor_nominal")
    )

    # Read materialized Gold
    df_gold_mat = (
        spark.read.format("delta")
        .load(gp.gold_pbf_estados_df_geo_path)
        .where(F.col("Ano").cast("string").rlike(r"^\\d+$"))
        .withColumn("Ano", F.col("Ano").cast("int"))
        .select("Ano", "uf", "n_benef", "valor_nominal")
    )

    df_cmp = (
        df_gold_re.alias("re")
        .join(df_gold_mat.alias("sv"), on=["Ano", "uf"], how="inner")
        .select(
            "Ano",
            "uf",
            F.col("re.n_benef").alias("re_n_benef"),
            F.col("sv.n_benef").alias("sv_n_benef"),
            (F.col("re.n_benef") - F.col("sv.n_benef")).alias("diff_n_benef"),
            F.col("re.valor_nominal").alias("re_valor_nominal"),
            F.col("sv.valor_nominal").alias("sv_valor_nominal"),
            (F.col("re.valor_nominal") - F.col("sv.valor_nominal")).alias("diff_valor_nominal"),
        )
    )

    print()
    print("==============================================")
    print("Biggest diffs (recomputed from Silver vs Gold)")
    print("==============================================")
    (
        df_cmp.where((F.col("diff_n_benef") != 0) | (F.abs(F.col("diff_valor_nominal")) > F.lit(1e-8)))
        .orderBy(F.abs(F.col("diff_n_benef")).desc(), F.abs(F.col("diff_valor_nominal")).desc())
        .show(int(args.limit), truncate=False)
    )

    spark.stop()


if __name__ == "__main__":
    main()
