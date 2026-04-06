"""
Silver job: monthly totals by state (UF).

Builds the Silver table ``total_ano_mes_estados``, aggregated at (Ano, Mes, UF) from the
Bronze payments Delta table.

This table is used as the base for downstream Gold exports and the web visualizations.
"""

from __future__ import annotations

from pathlib import Path
from typing import Tuple

from pyspark.sql import DataFrame, functions as F

from app.src.silver.common import (
    LakehousePaths,
    apply_nov_2021_origin_rule,
    build_delta_spark,
    parse_valor_parcela_decimal38,
    with_ano_mes_from_mes_competencia,
)


def build_total_ano_mes_estados(df_bronze_payments: DataFrame) -> DataFrame:
    """
    Build the Silver table ``total_ano_mes_estados``.

    Legacy reference: ``Passo2_dataPrep.R``:
      - group_by(mes_competencia, uf)
      - n = n_distinct(nis_favorecido) (beneficiary identifier)
      - total_estado = sum(valor_parcela)

    Project rule (Nov/2021):
      - for 2021-11, use ONLY ``origin="PBF_AUX_SUM"`` (exclude PBF/AUX raw rows)
      - for other months, exclude ``origin="PBF_AUX_SUM"`` to avoid double counting

    Args:
        df_bronze_payments: Bronze payments DataFrame.

    Returns:
        DataFrame with columns:
          - Ano, Mes
          - uf, mes_competencia
          - n (distinct beneficiaries)
          - total_estado (sum of ``valor_parcela`` as Decimal)
    """
    df = apply_nov_2021_origin_rule(df_bronze_payments)
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


def write_total_ano_mes_estados(
    *,
    lakehouse_root: Path = Path("lakehouse"),
    mode: str = "overwrite",
    partition_by: Tuple[str, ...] = ("Ano", "Mes"),
) -> None:
    """
    Materialize ``total_ano_mes_estados`` into the lakehouse Silver area.

    Args:
        lakehouse_root: Lakehouse root directory.
        mode: Spark write mode (default ``overwrite``).
        partition_by: Delta partition columns (default partitions by year+month).
    """
    spark = build_delta_spark("silver-total-ano-mes-estados")
    paths = LakehousePaths(lakehouse_root=lakehouse_root)

    df_bronze = spark.read.format("delta").load(paths.bronze_payments_path)
    df_out = build_total_ano_mes_estados(df_bronze)

    paths.silver_root.mkdir(parents=True, exist_ok=True)
    (
        df_out.write.format("delta")
        .mode(mode)
        .partitionBy(*partition_by)
        .save(str(paths.silver_root / "total_ano_mes_estados"))
    )

    spark.stop()


if __name__ == "__main__":
    write_total_ano_mes_estados()
