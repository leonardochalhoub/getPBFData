from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Tuple

import requests
from pyspark.sql import DataFrame, SparkSession, Window, functions as F, types as T

from app.src.shared.spark import SparkConfig, SparkFactory
from app.src.silver.common import LakehousePaths


BCB_IPCA_SERIES_CODE = 433  # IPCA - variação mensal (%)


@dataclass(frozen=True)
class GoldPaths:
    lakehouse_root: Path = Path("lakehouse")

    @property
    def gold_root(self) -> Path:
        return self.lakehouse_root / "gold"

    @property
    def silver_total_ano_mes_estados_path(self) -> str:
        return str(self.lakehouse_root / "silver" / "total_ano_mes_estados")

    @property
    def silver_populacao_uf_ano_path(self) -> str:
        return str(self.lakehouse_root / "silver" / "populacao_uf_ano")

    @property
    def gold_pbf_estados_df_geo_path(self) -> str:
        return str(self.gold_root / "pbf_estados_df_geo")


def _download_ipca_series_json(*, timeout_s: int = 60) -> list[dict]:
    """
    Download IPCA monthly variation (%) from Banco Central do Brasil (SGS API).

    Endpoint returns a list of:
      [{"data":"01/01/1980","valor":"6.62"}, ...]
    where 'valor' is percent change in the month.
    """
    url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{BCB_IPCA_SERIES_CODE}/dados?formato=json"
    r = requests.get(url, timeout=timeout_s)
    r.raise_for_status()
    return r.json()


def _ipca_json_to_monthly_index_df(spark: SparkSession, ipca_json: list[dict]) -> DataFrame:
    """
    Convert monthly variation (%) into a monthly price index where:
      - index_{2021-12} == 1.0 (reference)
      - index_t = product_{m=t+1..2021-12} (1 + ipca_m/100)

    This lets us inflate a value observed at year t (annual) by joining on year and using
    the December index for each year.
    """
    schema = T.StructType(
        [
            T.StructField("data", T.StringType(), False),
            T.StructField("valor", T.StringType(), False),
        ]
    )

    df = spark.createDataFrame(ipca_json, schema=schema)

    # Parse "dd/mm/YYYY"
    df = df.withColumn("dt", F.to_date(F.col("data"), "dd/MM/yyyy")).drop("data")

    # Monthly factor (1 + pct/100)
    df = df.withColumn(
        "ipca_pct",
        F.regexp_replace(F.col("valor"), ",", ".").cast("double"),
    ).drop("valor")
    df = df.withColumn("factor", F.lit(1.0) + (F.col("ipca_pct") / F.lit(100.0)))

    df = df.withColumn("year", F.year("dt").cast("int")).withColumn("month", F.month("dt").cast("int"))

    return df.select("dt", "year", "month", "factor")


def build_pbf_estados_df_geo(
    df_total_ano_mes_estados: DataFrame,
    *,
    df_populacao_estados: DataFrame,
    df_states_geo: DataFrame,
    df_deflators_to_2021: DataFrame,
) -> DataFrame:
    """
    Gold table: pbf_estados_df_geo

    Port of legacy Passo3_postProc.R for the *state* part, with geo join.

    Inputs:
    - silver total_ano_mes_estados (Ano, Mes, uf, n, total_estado, ...)
    - df_populacao_estados: (Ano, uf, populacao)
    - df_states_geo: (uf, name_state, geom/geo columns...) -> we keep minimal subset
    - df_deflators_to_2021: (Ano, deflator_to_2021)

    Output:
      Ano, uf, n_benef,
      valor_nominal (billions of BRL in the original year),
      valor_2021 (billions of BRL inflated to 2021 reais using IPCA from BCB/SGS),
      populacao, pbfPerBenef, pbfPerCapita, <geo cols>
    """
    # 1) Reduce ano-mes to ano (UF × Ano)
    df_year = (
        df_total_ano_mes_estados.groupBy("Ano", "uf")
        .agg(
            F.max("n").cast("long").alias("n_benef"),
            (F.sum("total_estado") / F.lit(1e9)).alias("valor_nominal"),
        )
        .join(df_populacao_estados, on=["Ano", "uf"], how="left")
    )

    # 2) Inflate to 2021 reais (annual approximation using December deflators)
    df_year = df_year.join(df_deflators_to_2021, on=["Ano"], how="left").withColumn(
        "valor_2021", F.col("valor_nominal") * F.col("deflator_to_2021")
    )

    # 3) Derived metrics (back to BRL for per-beneficiary/per-capita), computed using 2021-adjusted value
    df_year = df_year.withColumn("pbfPerBenef", (F.col("valor_2021") * F.lit(1e9)) / F.col("n_benef")).withColumn(
        "pbfPerCapita", (F.col("valor_2021") * F.lit(1e9)) / F.col("populacao")
    )

    # 4) Aggregated row per UF across the observed year range in the input
    year_bounds = df_total_ano_mes_estados.select(
        F.min("Ano").alias("min_ano"),
        F.max("Ano").alias("max_ano"),
    )
    min_ano = year_bounds.collect()[0]["min_ano"]
    max_ano = year_bounds.collect()[0]["max_ano"]

    # Aggregated UF row across years (within each UF)
    df_ag = (
        df_year.groupBy("uf")
        .agg(
            F.sum("n_benef").cast("long").alias("n_benef"),
            F.round(F.sum("valor_nominal"), 2).alias("valor_nominal"),
            F.round(F.sum("valor_2021"), 2).alias("valor_2021"),
            F.sum("populacao").cast("long").alias("populacao"),
        )
        .withColumn("pbfPerBenef", F.round(F.col("valor_2021") * F.lit(1e9) / F.col("n_benef"), 2))
        .withColumn("pbfPerCapita", F.round(F.col("valor_2021") * F.lit(1e9) / F.col("populacao"), 2))
        .withColumn("Ano", F.lit(f"Agregado {min_ano}-{max_ano}"))
    )

    # Output: keep both nominal and 2021-adjusted value columns
    df_year_sel = df_year.select(
        F.col("Ano").cast("string").alias("Ano"),
        "uf",
        "n_benef",
        "valor_nominal",
        "valor_2021",
        "populacao",
        "pbfPerBenef",
        "pbfPerCapita",
    )
    df_ag_sel = df_ag.select(
        "Ano",
        "uf",
        "n_benef",
        "valor_nominal",
        "valor_2021",
        "populacao",
        "pbfPerBenef",
        "pbfPerCapita",
    )

    df_out = df_year_sel.unionByName(df_ag_sel)

    # 5) Geo join (R uses geobr::read_state and joins select(2,5,6) by uf == abbrev_state)
    # Here we assume df_states_geo already has 'uf' as the abbreviation key.
    geo_cols = [c for c in df_states_geo.columns if c != "uf"]
    df_out = df_out.join(df_states_geo.select(["uf"] + geo_cols), on=["uf"], how="left")

    return df_out


def _build_year_december_deflators_to_2021(spark: SparkSession, *, end_year: int) -> DataFrame:
    """
    Build a DataFrame:
      year, deflator_to_2021

    We approximate annual adjustment by using the December cumulative index for each year.
    deflator_to_2021 = index_2021_12 / index_year_12
    With index_2021_12 normalized to 1.0, this is 1 / index_year_12.

    Note: This differs slightly from priceR's adjust_for_inflation behavior (which can use exact dates),
    but it is a consistent, reproducible approach using monthly IPCA.
    """
    ipca_json = _download_ipca_series_json()
    df_monthly = _ipca_json_to_monthly_index_df(spark, ipca_json)

    # Keep up to (end_year + 1)-12 so we can compute a December index for end_year as well.
    # Reason: the monthly IPCA series may not yet have data for Dec of the current/latest year.
    # If we only keep up to end_year-12 and that month is missing, end_year will disappear from df_dec.
    # By allowing one extra year, we still compute cum_index forward as far as the source data allows,
    # and we can later fill missing years.
    df_monthly = df_monthly.where(F.col("dt") <= F.to_date(F.lit(f"{end_year + 1}-12-01")))

    # Cumulative index from the start to each month: cumprod(factor)
    # This uses a window without partition. That's OK because IPCA is a single time-series.
    # To avoid Spark's "No Partition Defined" warning (and forced single partition),
    # we explicitly add a constant partition key.
    df_monthly = df_monthly.withColumn("_series", F.lit(1))

    w = (
        Window.partitionBy("_series")
        .orderBy(F.col("dt").asc())
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    # Avoid log(0)
    df_monthly = df_monthly.withColumn("log_factor", F.log(F.col("factor")))
    df_monthly = df_monthly.withColumn("cum_index", F.exp(F.sum("log_factor").over(w)))

    # December index per year
    df_dec = df_monthly.where(F.col("month") == F.lit(12)).select(
        F.col("year").alias("Ano"),
        F.col("cum_index").alias("index_dec"),
    )

    # Normalize so that 2021-12 == 1.0 (base year)
    index_2021 = df_dec.where(F.col("Ano") == F.lit(2021)).select("index_dec").limit(1)
    index_2021_val = index_2021.collect()[0][0]

    df_dec = df_dec.withColumn("deflator_to_2021", F.lit(float(index_2021_val)) / F.col("index_dec")).select(
        "Ano", "deflator_to_2021"
    )

    # Ensure we have a deflator row for every year up to end_year.
    # If IPCA hasn't published December for the latest year yet, we fall back to using the latest available year.
    max_avail_year = int(df_dec.select(F.max("Ano")).collect()[0][0])
    df_dec = df_dec.withColumn(
        "Ano",
        F.when(F.col("Ano") > F.lit(end_year), F.lit(end_year)).otherwise(F.col("Ano")),
    )

    # Build full year grid and forward-fill from the latest available deflator (usually previous year).
    years = list(range(2013, end_year + 1))
    df_years = spark.createDataFrame([(y,) for y in years], schema="Ano int")

    df_filled = (
        df_years.join(df_dec, on="Ano", how="left")
        .withColumn("_series", F.lit(1))
        .withColumn(
            "_yr_order",
            F.col("Ano"),
        )
    )

    w_ffill = (
        Window.partitionBy("_series")
        .orderBy(F.col("_yr_order").asc())
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    df_filled = df_filled.withColumn("deflator_to_2021", F.last("deflator_to_2021", ignorenulls=True).over(w_ffill)).drop(
        "_series", "_yr_order"
    )

    return df_filled


def _download_ibge_uf_population_estimates(
    *,
    start_year: int,
    end_year: int,
    timeout_s: int = 60,
) -> list[dict]:
    """
    Download yearly UF population estimates from IBGE Agregados API (SIDRA).

    Using:
      agregado=6579 (População residente estimada)
      variavel=9324
      localidades=N3[all] (UF level)
      periodos=start_year-end_year (inclusive range)

    Returns the raw JSON list from IBGE.
    """
    if start_year > end_year:
        raise ValueError(f"start_year must be <= end_year. Got {start_year=} {end_year=}")

    # IBGE allows ranges in 'periodos' (e.g. 2013-2025)
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

    IBGE UF ids are numeric strings:
      11=RO, 12=AC, ... 53=DF
    We map them to standard UF abbreviations.
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

    # Expected shape:
    # [ { "resultados": [ { "series": [ { "localidade": {"id": "12", ...}, "serie": {"2013":"776463", ...}} ... ] } ] } ]
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
                    if ano_str is None or val_str is None:
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


def _read_populacao_estados_from_silver(
    spark: SparkSession,
    *,
    silver_populacao_uf_ano_path: str,
    start_year: int,
    end_year: int,
) -> DataFrame:
    """
    Read UF population from the *silver* lakehouse table (Delta):
      lakehouse/silver/populacao_uf_ano

    This table is expected to already be complete for every UF×Ano in [start_year, end_year]
    (filled from IBGE + linear interpolation/extrapolation in the silver job).

    We still filter to the gold job's observed year range to keep joins tight.
    """
    df = spark.read.format("delta").load(silver_populacao_uf_ano_path)
    return df.where((F.col("Ano") >= F.lit(start_year)) & (F.col("Ano") <= F.lit(end_year))).select("Ano", "uf", "populacao")


def _load_states_geo_stub(spark: SparkSession) -> DataFrame:
    """
    Placeholder for state geo data.

    In R this comes from geobr::read_state(year=2019). In this PySpark pipeline,
    we need an equivalent source (GeoJSON/Shapefile/Parquet with geometry) already available.

    For now, return a minimal df with uf only; the join will keep schema stable.
    """
    schema = T.StructType([T.StructField("uf", T.StringType(), False)])
    return spark.createDataFrame([], schema=schema)


def write_pbf_estados_df_geo(
    *,
    lakehouse_root: Path = Path("lakehouse"),
    mode: str = "overwrite",
    partition_by: Tuple[str, ...] = ("Ano",),
) -> None:
    spark = SparkFactory.create(SparkConfig(app_name="gold-pbf-estados-df-geo", master="local[*]", shuffle_partitions=64))
    paths = GoldPaths(lakehouse_root=lakehouse_root)

    df_total = spark.read.format("delta").load(paths.silver_total_ano_mes_estados_path)

    # Determine year range from silver input
    bounds = df_total.select(F.min("Ano").alias("min_ano"), F.max("Ano").alias("max_ano")).collect()[0]
    min_ano = int(bounds["min_ano"])
    max_ano = int(bounds["max_ano"])

    # population (from silver lakehouse: IBGE official + linear fill)
    df_pop = _read_populacao_estados_from_silver(
        spark,
        silver_populacao_uf_ano_path=paths.silver_populacao_uf_ano_path,
        start_year=min_ano,
        end_year=max_ano,
    )

    # inflation deflators (from BCB IPCA series) - build up to max_ano so 2022..max_ano join works
    df_defl = _build_year_december_deflators_to_2021(spark, end_year=max_ano)

    # geo (stub for now)
    df_geo = _load_states_geo_stub(spark)

    df_out = build_pbf_estados_df_geo(
        df_total,
        df_populacao_estados=df_pop,
        df_states_geo=df_geo,
        df_deflators_to_2021=df_defl,
    )

    paths.gold_root.mkdir(parents=True, exist_ok=True)
    # Ensure true overwrite even if partition values changed (e.g. aggregated label)
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    # Schema evolves (e.g. adding valor_nominal + valor_2021). Use mergeSchema to migrate.
    (
        df_out.write.format("delta")
        .mode(mode)
        .option("mergeSchema", "true")
        .partitionBy(*partition_by)
        .save(paths.gold_pbf_estados_df_geo_path)
    )

    spark.stop()


if __name__ == "__main__":
    write_pbf_estados_df_geo()
