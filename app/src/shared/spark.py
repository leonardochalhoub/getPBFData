"""
Spark/Delta bootstrap helpers.

This module centralizes Spark session configuration used by the ingestion/transformation
pipeline. It intentionally keeps configuration explicit and code-only (no external config
files) to make runs reproducible.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


@dataclass(frozen=True)
class SparkConfig:
    """
    Configuration used to build a SparkSession.

    Attributes:
        app_name: Name shown in Spark UI.
        master: Spark master string (e.g. ``local[*]``). If ``None``, Spark uses its default.
        shuffle_partitions: Default value for ``spark.sql.shuffle.partitions``.
        timezone: Session timezone used by Spark SQL for timestamp/date handling.
    """

    app_name: str = "pbf-pipeline"
    master: Optional[str] = None
    shuffle_partitions: int = 200
    timezone: str = "America/Sao_Paulo"


class SparkFactory:
    """Factory for creating a configured :class:`pyspark.sql.SparkSession`."""

    @staticmethod
    def create(cfg: SparkConfig) -> SparkSession:
        """
        Create a SparkSession configured for Delta Lake.

        The defaults are tuned for local execution and large file ingestion, and include
        Delta Lake extensions/catalog configuration.

        Args:
            cfg: Session configuration.

        Returns:
            A ready-to-use SparkSession instance.
        """
        builder = (
            SparkSession.builder.appName(cfg.app_name)
            .config("spark.sql.session.timeZone", cfg.timezone)
            .config("spark.sql.shuffle.partitions", str(cfg.shuffle_partitions))
            # safer defaults for local ingestion of huge CSVs
            .config("spark.sql.files.maxPartitionBytes", str(64 * 1024 * 1024))  # 64MB
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.driver.memory", "8g")
            .config("spark.executor.memory", "8g")
            # Delta Lake
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        )
        if cfg.master:
            builder = builder.master(cfg.master)

        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        return spark
