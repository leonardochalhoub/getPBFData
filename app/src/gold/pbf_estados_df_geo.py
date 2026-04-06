"""
Gold job: PBF annual state panel with geography and 2021-R$ adjustment.

This module builds the main exported dataset used by the web map/chart: an UF×Year panel
with beneficiaries, nominal amounts, inflation-adjusted amounts (to Dec/2021 reais), and
derived metrics (per beneficiary and per capita). It also appends an aggregated UF row
summing across the observed year range.

Sources:
  - Payments: Bronze -> Silver ``total_ano_mes_estados`` (derived from official payment ZIPs)
  - Population: IBGE/SIDRA (Agregados 6579, variável 9324, N3=UF) via Silver ``populacao_uf_ano``
  - Inflation: BCB/SGS IPCA monthly variation (series 433). Annual deflator uses December
    index and is normalized so that Dec/2021 == 1.0.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Tuple

from app.src.silver.common import LakehousePaths

import requests
from pyspark.sql import DataFrame, SparkSession, Window, functions as F, types as T

from app.src.shared.spark import SparkConfig, SparkFactory

BCB_IPCA_SERIES_CODE = 433  # IPCA - variação mensal (%)


@dataclass(frozen=True)
class GoldPaths:
    """
    Path helpers for Gold outputs.

    Attributes:
        lakehouse_root: Root folder of the lakehouse.
    """

    lakehouse_root: Path = Path("lakehouse")

    @property
    def gold_root(self) -> Path:
        """Root directory for Gold tables."""
        return self.lakehouse_root / "gold"

    @property
    def silver_total_ano_mes_estados_path(self) -> str:
        """Delta path for Silver ``total_ano_mes_estados``."""
        return str(self.lakehouse_root / "silver" / "total_ano_mes_estados")

    @property
    def silver_populacao_uf_ano_path(self) -> str:
        """Delta path for Silver ``populacao_uf_ano`` (IBGE population)."""
        return str(self.lakehouse_root / "silver" / "populacao_uf_ano")

    @property
    def gold_pbf_estados_df_geo_path(self) -> str:
        """Delta path for Gold ``pbf_estados_df_geo``."""
        return str(self.gold_root / "pbf_estados_df_geo")


def _download_ipca_series_json(*, timeout_s: int = 60) -> list[dict]:
    """
    Download IPCA monthly variation (%) from Banco Central do Brasil (SGS API).

    Args:
        timeout_s: HTTP timeout in seconds.

    Returns:
        List of dicts like ``{"data":"01/01/1980","valor":"6.62"}`` where ``valor`` is the
        percent change in the month.

    Raises:
        requests.HTTPError: If the API request fails.
    """
    url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{BCB_IPCA_SERIES_CODE}/dados?formato=json"
    r = requests.get(url, timeout=timeout_s)
    r.raise_for_status()
    return r.json()


def _ipca_json_to_monthly_index_df(spark: SparkSession, ipca_json: list[dict]) -> DataFrame:
    """
    Convert IPCA monthly variation into a monthly multiplicative factor table.

    The returned DataFrame contains one row per month with a numeric factor:

        ``factor = 1 + (ipca_pct / 100)``

    A cumulative index is later computed using a windowed cumulative product.

    Args:
        spark: Spark session used to create the DataFrame.
        ipca_json: JSON payload as returned by :func:`_download_ipca_series_json`.

    Returns:
        DataFrame with columns ``dt, year, month, factor``.
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
    Build the Gold table ``pbf_estados_df_geo``.

    This is a port of legacy ``Passo3_postProc.R`` for the *state* part, adding a geo join.

    Args:
        df_total_ano_mes_estados: Silver ``total_ano_mes_estados`` DataFrame.
        df_populacao_estados: Population by UF/year (``Ano, uf, populacao``).
        df_states_geo: Geo attributes keyed by UF abbreviation (must contain ``uf``).
        df_deflators_to_2021: Annual deflators (``Ano, deflator_to_2021``) built from IPCA
            using December indices normalized to Dec/2021.

    Returns:
        DataFrame with one row per UF/year plus an aggregated row per UF across the observed
        year range, containing:
          - beneficiaries (``n_benef``)
          - nominal value (billions BRL, original year)
          - value adjusted to 2021 reais (billions BRL)
          - per-beneficiary and per-capita values (in BRL)
          - geo attributes from ``df_states_geo``
    """
    # 1) Reduce ano-mes to ano (UF × Ano) for values
    df_year_val = (
        df_total_ano_mes_estados.groupBy("Ano", "uf")
        .agg(
            (F.sum("total_estado") / F.lit(1e9)).alias("valor_nominal"),
        )
        .join(df_populacao_estados, on=["Ano", "uf"], how="left")
    )

    # 1b) Annual distinct beneficiaries from Silver.
    #
    # IMPORTANT: `total_ano_mes_estados` contains:
    # - `n`: monthly distinct beneficiaries
    # - `n_ano`: annual distinct beneficiaries repeated on each month row
    #
    # Gold must use `n_ano` (annual distinct), NOT `n` (monthly distinct).
    df_year_benef = (
        df_total_ano_mes_estados.select("Ano", "uf", "n_ano")
        .distinct()
        .withColumnRenamed("n_ano", "n_benef")
        .withColumn("n_benef", F.col("n_benef").cast("long"))
    )

    df_year = df_year_val.join(df_year_benef, on=["Ano", "uf"], how="left")

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
    #
    # Important: population is a stock (level), not a flow. Summing population across years is incorrect.
    # For the aggregated period we use the average population across the years to compute per-capita.
    #
    # Note: beneficiaries and values are flows here, so summing across years is expected.
    df_ag = (
        df_year.groupBy("uf")
        .agg(
            F.sum("n_benef").cast("long").alias("n_benef"),
            F.round(F.sum("valor_nominal"), 2).alias("valor_nominal"),
            F.round(F.sum("valor_2021"), 2).alias("valor_2021"),
            F.round(F.avg("populacao"), 0).cast("long").alias("populacao"),
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
    Build annual deflators to convert values into Dec/2021 reais.

    The approach uses the monthly IPCA factors (series 433) to compute a cumulative index.
    Annual adjustment is approximated using each year's December index:

        ``deflator_to_2021 = index_dec_2021 / index_dec_year``

    Since the index is later normalized so that Dec/2021 == 1.0, this becomes:

        ``deflator_to_2021 = 1 / index_dec_year``

    Args:
        spark: Spark session.
        end_year: Last year we need a deflator for (inclusive).

    Returns:
        DataFrame ``(Ano:int, deflator_to_2021:double)`` covering years 2013..end_year
        (forward-filled if the latest December is not yet available).
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
    Download yearly UF population estimates from IBGE SIDRA Agregados API.

    Note:
        This helper is retained for backwards compatibility, but the preferred approach
        is to read population from Silver ``populacao_uf_ano`` (which already interpolates
        gaps). New code should generally avoid downloading here.

    Args:
        start_year: First year (inclusive).
        end_year: Last year (inclusive).
        timeout_s: HTTP timeout in seconds.

    Returns:
        Raw JSON list from IBGE.

    Raises:
        ValueError: If ``start_year > end_year``.
        requests.HTTPError: If the API request fails.
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
    Convert IBGE Agregados JSON into a Spark DataFrame (Ano, uf, populacao).

    Args:
        spark: Spark session.
        ibge_json: Raw payload from :func:`_download_ibge_uf_population_estimates`.

    Returns:
        Spark DataFrame with schema ``(Ano:int, uf:string, populacao:double)``.
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
    Read UF population from the Silver Delta table.

    Args:
        spark: Spark session.
        silver_populacao_uf_ano_path: Delta path to ``silver/populacao_uf_ano``.
        start_year: First year (inclusive).
        end_year: Last year (inclusive).

    Returns:
        Filtered DataFrame with columns ``Ano, uf, populacao``.
    """
    df = spark.read.format("delta").load(silver_populacao_uf_ano_path)
    return df.where((F.col("Ano") >= F.lit(start_year)) & (F.col("Ano") <= F.lit(end_year))).select("Ano", "uf", "populacao")


def _load_states_geo_stub(spark: SparkSession) -> DataFrame:
    """
    Return a minimal placeholder for state geo data.

    In the legacy R implementation this comes from ``geobr::read_state(year=2019)``.
    In this Python pipeline, we join geo attributes elsewhere (e.g. in the web layer).
    This stub keeps the Gold schema stable if no geo dataset is provided.

    Args:
        spark: Spark session.

    Returns:
        Empty DataFrame with a single ``uf`` column.
    """
    schema = T.StructType([T.StructField("uf", T.StringType(), False)])
    return spark.createDataFrame([], schema=schema)


def write_pbf_estados_df_geo(
    *,
    lakehouse_root: Path = Path("lakehouse"),
    mode: str = "overwrite",
    partition_by: Tuple[str, ...] = ("Ano",),
) -> None:
    """
    Materialize ``pbf_estados_df_geo`` into the lakehouse Gold area.

    Args:
        lakehouse_root: Lakehouse root directory.
        mode: Spark write mode (default ``overwrite``).
        partition_by: Delta partition columns (default partitions by year label).
    """
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
