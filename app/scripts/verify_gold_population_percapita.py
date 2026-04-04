from __future__ import annotations
"""
Verify gold table has population and per-capita values after switching to silver population.

Run:
  PYTHONPATH=. python app/scripts/verify_gold_population_percapita.py
"""

from app.src.silver.common import LakehousePaths, build_delta_spark
from pyspark.sql import functions as F


def main() -> None:
    spark = build_delta_spark("verify-gold-pbf-estados-df-geo")
    paths = LakehousePaths()

    gold_path = str(paths.lakehouse_root / "gold" / "pbf_estados_df_geo")
    df = spark.read.format("delta").load(gold_path)

    stats = df.agg(
        F.count("*").alias("rows"),
        F.sum(F.when(F.col("populacao").isNull(), 1).otherwise(0)).alias("null_pop"),
        F.sum(F.when(F.col("pbfPerCapita").isNull(), 1).otherwise(0)).alias("null_pbfPerCapita"),
    ).collect()[0]

    print("GOLD_PATH", gold_path)
    print("rows", int(stats["rows"]))
    print("null_pop", int(stats["null_pop"]))
    print("null_pbfPerCapita", int(stats["null_pbfPerCapita"]))

    print("\nSample rows (Ano numeric only):")
    (
        df.where(F.col("Ano").rlike(r"^\\d+$"))
        .select("Ano", "uf", "valor_2021", "populacao", "pbfPerCapita")
        .orderBy(F.col("Ano").cast("int").asc(), F.col("uf").asc())
        .show(20, truncate=False)
    )

    spark.stop()


if __name__ == "__main__":
    main()
