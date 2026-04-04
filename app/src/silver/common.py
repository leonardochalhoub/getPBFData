from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import DataFrame, SparkSession, functions as F, types as T


@dataclass(frozen=True)
class LakehousePaths:
    lakehouse_root: Path = Path("lakehouse")

    @property
    def bronze_payments_path(self) -> str:
        return str(self.lakehouse_root / "bronze" / "payments")

    @property
    def silver_root(self) -> Path:
        return self.lakehouse_root / "silver"


def build_delta_spark(app_name: str) -> SparkSession:
    """
    Build a local SparkSession configured for Delta.

    Defaults are tuned to avoid local-mode OOM during wide aggregations:
    - larger driver heap
    - fewer shuffle partitions
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
    Apply the 2021-11 rule:
    - keep ONLY origin='PBF_AUX_SUM' for 2021-11
    - for other months, drop origin='PBF_AUX_SUM' (to avoid double counting)
    """
    is_2021_11 = (F.col("ano") == F.lit(2021)) & (F.col("mes") == F.lit(11))
    return df.where(
        (is_2021_11 & (F.col("origin") == F.lit("PBF_AUX_SUM")))
        | (~is_2021_11 & (F.col("origin") != F.lit("PBF_AUX_SUM")))
    )


def parse_valor_parcela_decimal38(df: DataFrame, *, input_col: str = "valor_parcela", output_col: str = "valor_parcela_dec") -> DataFrame:
    """Parse '800,00' -> Decimal(38,2)."""
    dec = T.DecimalType(38, 2)
    return df.withColumn(output_col, F.regexp_replace(F.col(input_col), ",", ".").cast(dec))


def with_ano_mes_from_mes_competencia(df: DataFrame) -> DataFrame:
    """Derive Ano/Mes from mes_competencia (YYYYMM) like legacy R."""
    return (
        df.withColumn("Ano", F.substring(F.col("mes_competencia"), 1, 4).cast("int"))
        .withColumn("Mes", F.substring(F.col("mes_competencia"), 5, 2).cast("int"))
    )
