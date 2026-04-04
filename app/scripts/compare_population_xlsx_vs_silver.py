from __future__ import annotations
"""
Compare population from:
- arquivos_aux/populacao.xlsx (wide, 2013..2023)
vs
- lakehouse/silver/populacao_uf_ano (delta, built from IBGE + linear fill, typically 2013..2026)

Usage:
  python app/scripts/compare_population_xlsx_vs_silver.py
"""

from pathlib import Path

import pandas as pd
from pyspark.sql import functions as F

from app.src.silver.common import LakehousePaths, build_delta_spark


def main() -> None:
    # --- Load Excel (wide) and convert to long: (uf, Ano, pop_xlsx)
    xlsx_path = Path("arquivos_aux/populacao.xlsx")
    raw = pd.read_excel(xlsx_path)
    raw.columns = [str(c) for c in raw.columns]

    year_cols = [c for c in raw.columns if c.isdigit()]
    if not year_cols:
        raise RuntimeError(f"No year columns found in {xlsx_path}. Columns={list(raw.columns)}")

    long = raw.melt(
        id_vars=["uf", "nome_estado"],
        value_vars=year_cols,
        var_name="Ano",
        value_name="pop_xlsx",
    )
    long["Ano"] = long["Ano"].astype(int)
    long["pop_xlsx"] = pd.to_numeric(long["pop_xlsx"], errors="coerce")
    long = long[["uf", "Ano", "pop_xlsx"]]

    # --- Load silver delta and convert to pandas: (uf, Ano, pop_silver)
    spark = build_delta_spark("compare-xlsx-vs-silver-pop")
    paths = LakehousePaths()
    df_silver = spark.read.format("delta").load(str(paths.silver_root / "populacao_uf_ano"))
    silver = df_silver.select("uf", "Ano", F.col("populacao").alias("pop_silver")).toPandas()
    silver["Ano"] = silver["Ano"].astype(int)
    silver["pop_silver"] = pd.to_numeric(silver["pop_silver"], errors="coerce")

    # --- Compare overlapping years
    min_year = max(int(long["Ano"].min()), int(silver["Ano"].min()))
    max_year = min(int(long["Ano"].max()), int(silver["Ano"].max()))

    a = long[long["Ano"].between(min_year, max_year)]
    b = silver[silver["Ano"].between(min_year, max_year)]

    m = a.merge(b, on=["uf", "Ano"], how="inner")
    m["diff"] = m["pop_silver"] - m["pop_xlsx"]
    m["abs_diff"] = m["diff"].abs()
    m["pct_diff"] = m["diff"] / m["pop_xlsx"]

    print("OVERLAP_YEARS", min_year, max_year)
    print("ROWS_COMPARED", len(m))

    p50 = float(m["pct_diff"].quantile(0.5))
    p90 = float(m["pct_diff"].quantile(0.9))
    p95 = float(m["pct_diff"].quantile(0.95))
    p99 = float(m["pct_diff"].quantile(0.99))
    max_abs = float(m["abs_diff"].max())

    print("MEDIAN_PCT_DIFF", p50)
    print("P90_PCT_DIFF", p90)
    print("P95_PCT_DIFF", p95)
    print("P99_PCT_DIFF", p99)
    print("MAX_ABS_DIFF", max_abs)

    top = m.sort_values("abs_diff", ascending=False).head(10)
    print("TOP10_ABS_DIFF")
    print(top[["uf", "Ano", "pop_xlsx", "pop_silver", "diff", "pct_diff"]].to_string(index=False))

    # --- Silver table completeness stats
    stats = df_silver.agg(
        F.count("*").alias("rows"),
        F.countDistinct("uf").alias("ufs"),
        F.min("Ano").alias("min_ano"),
        F.max("Ano").alias("max_ano"),
        F.sum(F.when(F.col("populacao").isNull(), 1).otherwise(0)).alias("null_pop"),
    ).collect()[0]

    min_ano = int(stats["min_ano"])
    max_ano = int(stats["max_ano"])
    ufs = int(stats["ufs"])
    rows = int(stats["rows"])
    years = max_ano - min_ano + 1
    expected = ufs * years

    print("SILVER_ROWS", rows, "EXPECTED", expected, "UFS", ufs, "YEARS", f"{min_ano}-{max_ano}", "NULL_POP", int(stats["null_pop"]))

    spark.stop()

    # --- Conclusion heuristic
    consistent = (abs(p50) < 0.005) and (abs(p99) < 0.02)
    print("CONCLUSION", "CONSISTENT" if consistent else "DIFFERS")


if __name__ == "__main__":
    main()
