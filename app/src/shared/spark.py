from __future__ import annotations
from dataclasses import dataclass
from typing import Optional

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


@dataclass(frozen=True)
class SparkConfig:
    app_name: str = "pbf-pipeline"
    master: Optional[str] = None
    shuffle_partitions: int = 200
    timezone: str = "America/Sao_Paulo"


class SparkFactory:
    @staticmethod
    def create(cfg: SparkConfig) -> SparkSession:
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
