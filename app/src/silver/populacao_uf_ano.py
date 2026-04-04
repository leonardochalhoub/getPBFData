from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import Tuple

import requests
from pyspark.sql import DataFrame, SparkSession, Window, functions as F, types as T

from app.src.silver.common import LakehousePaths, build_delta_spark


def _download_ibge_uf_population_estimates(*, start_year: int, end_year: int, timeout_s: int = 60) -> list[dict]:
    """
    Download yearly UF population estimates from IBGE Agregados API (SIDRA).

    Using:
      agregado=6579 (População residente estimada)
      variavel=9324
      localidades=N3[all] (UF level)
      periodos=start_year-end_year (inclusive range)
    """
    if start_year > end_year:
        raise ValueError(f"start_year must be <= end_year. Got {start_year=} {end_year=}")

    url = (
        "https://servicodados.ibge.gov.br/api/v3/agregados/6579/"
        f"periodos/{start_year}-{end_year}/variaveis/9324"
        "?localidades=N3[all]"
    )
    r = requests.get(url, timeout=timeout_s)
    r.raise_for_status()
    return r.json()


def _ibge_population_json_to_df(spark: SparkSession, ibge_json: list[dict]) -> DataFrame:
    """
    Convert IBGE Agregados JSON into Spark DF (Ano, uf, populacao).
    """
    uf_id_to_sigla = {
        "11": "RO",
        "12": "AC",
        "13": "AM",
        "14": "RR",
        "15": "PA",
        "16": "AP",
        "17": "TO",
        "21": "MA",
        "22": "PI",
        "23": "CE",
        "24": "RN",
        "25": "PB",
        "26": "PE",
        "27": "AL",
        "28": "SE",
        "29": "BA",
        "31": "MG",
        "32": "ES",
        "33": "RJ",
        "35": "SP",
        "41": "PR",
        "42": "SC",
        "43": "RS",
        "50": "MS",
        "51": "MT",
        "52": "GO",
        "53": "DF",
    }

    out_rows: list[tuple[int, str, float]] = []
    for item in ibge_json or []:
        for res in item.get("resultados", []):
            for s in res.get("series", []):
                loc = s.get("localidade", {}) or {}
                uf_id = str(loc.get("id", "")).strip()
                uf = uf_id_to_sigla.get(uf_id)
                if not uf:
                    continue
                serie = s.get("serie", {}) or {}
                for ano_str, val_str in serie.items():
                    if val_str in (None, "", "..."):
                        continue
                    try:
                        ano = int(str(ano_str))
                        pop = float(str(val_str))
                    except Exception:
                        continue
                    out_rows.append((ano, uf, pop))

    schema = T.StructType(
        [
            T.StructField("Ano", T.IntegerType(), False),
            T.StructField("uf", T.StringType(), False),
            T.StructField("populacao", T.DoubleType(), True),
        ]
    )
    return spark.createDataFrame(out_rows, schema=schema)


def build_populacao_uf_ano(*, spark: SparkSession, start_year: int, end_year: int) -> DataFrame:
    """
    Silver table: populacao_uf_ano

    Produces a complete UF x Year panel for [start_year, end_year], using:
    - official IBGE UF yearly population estimates (Agregados API)
    - linear interpolation for internal gaps
    - linear extrapolation for edge gaps
    """
    ibge_json = _download_ibge_uf_population_estimates(start_year=start_year, end_year=end_year)
    df_pop_raw = _ibge_population_json_to_df(spark, ibge_json)

    # Full year grid for each UF
    df_years = spark.range(start_year, end_year + 1).select(F.col("id").cast("int").alias("Ano"))
    df_ufs = df_pop_raw.select("uf").distinct()
    df_grid = df_ufs.crossJoin(df_years)

    df_pop = df_grid.join(df_pop_raw, on=["uf", "Ano"], how="left")

    w_left = (
        Window.partitionBy("uf")
        .orderBy(F.col("Ano").asc())
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    w_right = (
        Window.partitionBy("uf")
        .orderBy(F.col("Ano").asc())
        .rowsBetween(Window.currentRow, Window.unboundedFollowing)
    )

    df_pop = df_pop.withColumn(
        "_ano_left", F.last(F.when(F.col("populacao").isNotNull(), F.col("Ano")), ignorenulls=True).over(w_left)
    ).withColumn("_pop_left", F.last("populacao", ignorenulls=True).over(w_left))

    df_pop = df_pop.withColumn(
        "_ano_right", F.first(F.when(F.col("populacao").isNotNull(), F.col("Ano")), ignorenulls=True).over(w_right)
    ).withColumn("_pop_right", F.first("populacao", ignorenulls=True).over(w_right))

    df_pop = df_pop.withColumn(
        "populacao_filled",
        F.when(F.col("populacao").isNotNull(), F.col("populacao"))
        .when(
            (F.col("_ano_left").isNotNull())
            & (F.col("_ano_right").isNotNull())
            & (F.col("_ano_right") != F.col("_ano_left")),
            F.col("_pop_left")
            + (F.col("Ano") - F.col("_ano_left"))
            * (F.col("_pop_right") - F.col("_pop_left"))
            / (F.col("_ano_right") - F.col("_ano_left")),
        )
        .when(F.col("_pop_left").isNotNull(), F.col("_pop_left"))
        .when(F.col("_pop_right").isNotNull(), F.col("_pop_right"))
        .otherwise(F.lit(None).cast("double")),
    )

    return (
        df_pop.select("Ano", "uf", F.col("populacao_filled").alias("populacao"))
        .withColumn("populacao", F.round(F.col("populacao"), 0))
        .orderBy("uf", "Ano")
    )


def write_populacao_uf_ano(
    *,
    lakehouse_root: Path = Path("lakehouse"),
    start_year: int = 2013,
    end_year: int = 2026,
    mode: str = "overwrite",
    partition_by: Tuple[str, ...] = ("Ano",),
) -> None:
    spark = build_delta_spark("silver-populacao-uf-ano")
    paths = LakehousePaths(lakehouse_root=lakehouse_root)

    df_out = build_populacao_uf_ano(spark=spark, start_year=start_year, end_year=end_year)

    paths.silver_root.mkdir(parents=True, exist_ok=True)
    (
        df_out.write.format("delta")
        .mode(mode)
        .partitionBy(*partition_by)
        .save(str(paths.silver_root / "populacao_uf_ano"))
    )
    spark.stop()


if __name__ == "__main__":
    write_populacao_uf_ano()
