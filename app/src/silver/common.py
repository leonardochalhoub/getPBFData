"""
Shared helpers for Silver transformations.

The "Silver" layer performs cleaned / derived tables on top of the Bronze payments table.
This module contains small reusable utilities used by multiple Silver jobs.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import DataFrame, SparkSession, functions as F, types as T


@dataclass(frozen=True)
class LakehousePaths:
    """
    Canonical lakehouse layout.

    Attributes:
        lakehouse_root: Root folder of the lakehouse. Defaults to ``lakehouse`` in the repo.
    """

    lakehouse_root: Path = Path("lakehouse")

    @property
    def bronze_payments_path(self) -> str:
        """
        Delta path for the Bronze payments table.

        Returns:
            String path used in Spark ``.load()`` / ``.save()`` calls.
        """
        return str(self.lakehouse_root / "bronze" / "payments")

    @property
    def silver_root(self) -> Path:
        """
        Root directory for Silver tables.

        Returns:
            Path object under ``lakehouse_root/silver``.
        """
        return self.lakehouse_root / "silver"


def build_delta_spark(app_name: str) -> SparkSession:
    """
    Build a local SparkSession configured for Delta Lake.

    Defaults are tuned to avoid local-mode OOM during wide aggregations:
    - larger driver heap
    - fewer shuffle partitions
    - adaptive query execution enabled

    Args:
        app_name: Spark application name.

    Returns:
        Configured SparkSession.
    """
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # Memory / shuffle tuning for local runs
        .config("spark.driver.memory", "8g")
        .config("spark.sql.shuffle.partitions", "64")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def apply_nov_2021_origin_rule(df: DataFrame) -> DataFrame:
    """
    Apply the Nov/2021 deduplication rule for the combined PBF+AUX partition.

    In 2021-11 there are two official files (PBF and AUX). Bronze ingestion additionally
    creates a synthetic partition ``origin="PBF_AUX_SUM"`` containing a combined view.
    Downstream tables must:
      - keep ONLY ``origin="PBF_AUX_SUM"`` for 2021-11
      - drop ``origin="PBF_AUX_SUM"`` for all other months to avoid double counting

    Args:
        df: Input DataFrame with ``origin``, ``ano`` and ``mes`` columns.

    Returns:
        Filtered DataFrame with the rule applied.
    """
    is_2021_11 = (F.col("ano") == F.lit(2021)) & (F.col("mes") == F.lit(11))
    return df.where(
        (is_2021_11 & (F.col("origin") == F.lit("PBF_AUX_SUM")))
        | (~is_2021_11 & (F.col("origin") != F.lit("PBF_AUX_SUM")))
    )


def parse_valor_parcela_decimal38(
    df: DataFrame, *, input_col: str = "valor_parcela", output_col: str = "valor_parcela_dec"
) -> DataFrame:
    """
    Parse a Brazilian-formatted money value column into Decimal(38, 2).

    Example:
        ``"800,00"`` -> ``Decimal(800.00)``

    Args:
        df: Input DataFrame.
        input_col: Name of the source string column.
        output_col: Name of the output decimal column.

    Returns:
        DataFrame with an extra decimal column.
    """
    dec = T.DecimalType(38, 2)
    return df.withColumn(output_col, F.regexp_replace(F.col(input_col), ",", ".").cast(dec))


def with_ano_mes_from_mes_competencia(df: DataFrame) -> DataFrame:
    """
    Derive ``Ano`` and ``Mes`` from ``mes_competencia`` (YYYYMM), matching legacy R logic.

    Args:
        df: Input DataFrame with ``mes_competencia`` column.

    Returns:
        DataFrame with integer columns ``Ano`` and ``Mes``.
    """
    return (
        df.withColumn("Ano", F.substring(F.col("mes_competencia"), 1, 4).cast("int"))
        .withColumn("Mes", F.substring(F.col("mes_competencia"), 5, 2).cast("int"))
    )
