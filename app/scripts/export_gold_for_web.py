from __future__ import annotations
"""
Export gold layer (pbf_estados_df_geo) into a compact JSON for a local webpage.

Output:
  exports/web/gold_pbf_estados_df_geo.json

Run:
  PYTHONPATH=. python app/scripts/export_gold_for_web.py
"""

import json
from pathlib import Path

from pyspark.sql import functions as F

from app.src.silver.common import LakehousePaths, build_delta_spark


def main() -> None:
    spark = build_delta_spark("export-gold-for-web")
    paths = LakehousePaths()

    gold_path = str(paths.lakehouse_root / "gold" / "pbf_estados_df_geo")
    df = spark.read.format("delta").load(gold_path)

    # Keep only per-year rows (exclude "Agregado ...")
    # NOTE: Spark rlike uses Java regex. Here we want: start + digits + end.
    # Using a normal python string avoids confusion with raw-string escaping.
    df = df.where(F.col("Ano").cast("string").rlike("^\\d+$")).withColumn("Ano", F.col("Ano").cast("int"))

    # Drop 2026 from the web export series (requested for the dashboard)
    df = df.where(F.col("Ano") <= F.lit(2025))

    # Select minimal fields needed by the frontend
    df = df.select(
        "Ano",
        "uf",
        F.col("valor_nominal").cast("double").alias("valor_nominal"),
        F.col("valor_2021").cast("double").alias("valor_2021"),
        F.col("populacao").cast("double").alias("populacao"),
        F.col("pbfPerBenef").cast("double").alias("pbfPerBenef"),
        F.col("pbfPerCapita").cast("double").alias("pbfPerCapita"),
    )

    # Collect to driver (small dataset: 27 UFs * years)
    rows = [r.asDict(recursive=True) for r in df.orderBy("Ano", "uf").collect()]

    out_path = Path("exports/web/gold_pbf_estados_df_geo.json")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(rows, ensure_ascii=False), encoding="utf-8")

    print("WROTE", str(out_path), "rows", len(rows))
    spark.stop()


if __name__ == "__main__":
    main()
