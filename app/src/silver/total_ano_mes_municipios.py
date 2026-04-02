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


def build_total_ano_mes_municipio(df_bronze_payments: DataFrame) -> DataFrame:
    """
    Silver table: total_ano_mes_municipio

    Legacy reference: Passo2_dataPrep.R
    - group_by(mes_competencia, nome_municipio, uf)
    - n = n_distinct(nome_favorecido)
    - total_municipio = sum(valor_parcela)

    Project rule:
    - For 2021-11, use ONLY origin='PBF_AUX_SUM' (exclude PBF/AUX raw rows)
    - For other months, exclude origin='PBF_AUX_SUM' to avoid double counting
    """
    df = apply_nov_2021_origin_rule(df_bronze_payments)
    df = parse_valor_parcela_decimal38(df)

    out = (
        df.groupBy("mes_competencia", "nome_municipio", "uf")
        .agg(
            F.countDistinct("nome_favorecido").alias("n"),
            F.sum(F.col("valor_parcela_dec")).alias("total_municipio"),
        )
        .transform(with_ano_mes_from_mes_competencia)
        .select("Ano", "Mes", "uf", "nome_municipio", "mes_competencia", "n", "total_municipio")
    )
    return out


def write_total_ano_mes_municipio(
    *,
    lakehouse_root: Path = Path("lakehouse"),
    mode: str = "overwrite",
    partition_by: Tuple[str, ...] = ("Ano", "Mes"),
) -> None:
    spark = build_delta_spark("silver-total-ano-mes-municipio")
    paths = LakehousePaths(lakehouse_root=lakehouse_root)

    df_bronze = spark.read.format("delta").load(paths.bronze_payments_path)
    df_out = build_total_ano_mes_municipio(df_bronze)

    paths.silver_root.mkdir(parents=True, exist_ok=True)
    (
        df_out.write.format("delta")
        .mode(mode)
        .partitionBy(*partition_by)
        .save(str(paths.silver_root / "total_ano_mes_municipio"))
    )

    spark.stop()


if __name__ == "__main__":
    write_total_ano_mes_municipio()
