from __future__ import annotations

import re
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple

from pyspark.sql import DataFrame, SparkSession, functions as F


_MONTH_RE = re.compile(r"(?P<year>20\d{2})[_-]?(?P<month>0[1-9]|1[0-2])", re.IGNORECASE)
_SOURCE_RE = re.compile(r"^(?P<src>PBF|AUX_BR|AUX|NBF)[_-]", re.IGNORECASE)


@dataclass(frozen=True)
class BronzePaths:
    source_zips_dir: Path
    bronze_root: Path

    @property
    def bronze_payments_path(self) -> str:
        return str(self.bronze_root / "payments")


@dataclass(frozen=True)
class IngestOptions:
    encoding: str = "latin1"
    sep: str = ";"  # read_csv2
    header: bool = True
    infer_schema: bool = False  # prefer explicit schema + rescue column
    bad_records_path: Optional[str] = None


class ZipPaymentsBronzeIngestor:
    """Ingests monthly zip files into a Delta Lake Bronze table.

    Strategy:
    - Extracts the inner CSV to a local temp dir and reads with Spark.
    - Normalizes column names to Delta-friendly ASCII snake_case.
    - Fixes Nov/2021 Bolsa Família header swap issue (mes_competencia/mes_referencia).
    - Adds metadata:
        - ano, mes, competencia (yyyymm inferred from filename)
        - origin (AUX/PBF/NBF inferred from filename prefix)
        - source_zip, source_inner, ingest_ts
    - Writes append to Delta partitioned by origin/ano/mes.

    Important:
    - The R pipeline sums PBF + Auxílio Brasil for 2021-11 *implicitly* because it aggregates
      all CSVs together by mes_competencia (it reads every extracted CSV and then group_by/sum).
      In our Python pipeline we keep both origins in Bronze and will implement the “sum Nov/2021”
      rule in Silver/Gold by aggregating across origins.
    """

    def __init__(self, spark: SparkSession, paths: BronzePaths, opts: IngestOptions = IngestOptions()):
        self.spark = spark
        self.paths = paths
        self.opts = opts

    def _list_zip_files(self) -> list[Path]:
        zdir = self.paths.source_zips_dir
        if not zdir.exists():
            raise FileNotFoundError(f"Source zip dir not found: {zdir}")
        return sorted([p for p in zdir.iterdir() if p.is_file() and p.suffix.lower() == ".zip"])

    @staticmethod
    def _month_from_name(path: Path) -> Tuple[int, int]:
        m = _MONTH_RE.search(path.name)
        if not m:
            raise ValueError(f"Cannot infer year/month from filename: {path.name}")
        return int(m.group("year")), int(m.group("month"))

    @staticmethod
    def _origin_from_name(path: Path) -> str:
        """Infer origin from zip filename prefix (AUX/PBF/NBF)."""
        m = _SOURCE_RE.match(path.name)
        if not m:
            return "UNK"
        src = m.group("src").upper()
        if src in ("AUX", "AUX_BR"):
            return "AUX"
        if src == "PBF":
            return "PBF"
        if src == "NBF":
            return "NBF"
        return src

    @staticmethod
    def _find_inner_csv(zip_path: Path) -> str:
        with zipfile.ZipFile(zip_path) as zf:
            names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if not names:
            raise RuntimeError(f"No CSV found inside {zip_path}")
        # Prefer a single csv; otherwise pick the largest (common case: docs + csv)
        if len(names) == 1:
            return names[0]
        with zipfile.ZipFile(zip_path) as zf:
            sized = sorted(((n, zf.getinfo(n).file_size) for n in names), key=lambda x: x[1], reverse=True)
        return sized[0][0]

    def _read_zip_csv(self, zip_path: Path) -> Tuple[DataFrame, str]:
        """Extracts the inner CSV to a local temp dir and reads with Spark."""
        inner = self._find_inner_csv(zip_path)
        extract_dir = self.paths.bronze_root / "_tmp_extract" / zip_path.stem
        extract_dir.mkdir(parents=True, exist_ok=True)
        target = extract_dir / Path(inner).name

        if not target.exists() or target.stat().st_size == 0:
            with zipfile.ZipFile(zip_path) as zf:
                zf.extract(inner, path=extract_dir)
            # zipfile preserves inner directories; move to flat target if needed
            extracted = extract_dir / inner
            if extracted != target and extracted.exists():
                extracted.replace(target)

        df = (
            self.spark.read.format("csv")
            .option("header", str(self.opts.header).lower())
            .option("sep", self.opts.sep)
            .option("encoding", self.opts.encoding)
            .option("multiLine", "false")
            .option("quote", '"')
            .option("escape", '"')
            .option("mode", "PERMISSIVE")
            .load(str(target))
        )
        return df, inner

    @staticmethod
    def _normalize_cols(df: DataFrame) -> DataFrame:
        """Normalize to Delta-friendly snake_case ASCII column names."""
        renamed = df
        for c in df.columns:
            # Upper + trim first (some files have inconsistent case/spaces)
            raw = c.strip().upper()

            # Manual normalization for the problematic headers
            raw = raw.replace("MÊS", "MES")

            # Replace common separators/accents safely
            new = (
                raw.replace("Á", "A")
                .replace("À", "A")
                .replace("Â", "A")
                .replace("Ã", "A")
                .replace("É", "E")
                .replace("Ê", "E")
                .replace("Í", "I")
                .replace("Ó", "O")
                .replace("Ô", "O")
                .replace("Õ", "O")
                .replace("Ú", "U")
                .replace("Ç", "C")
            )

            # to snake-ish
            new = re.sub(r"[^A-Z0-9]+", "_", new).strip("_").lower()

            if new and new != c:
                renamed = renamed.withColumnRenamed(c, new)
        return renamed

    @staticmethod
    def _fix_nov_2021_header_swap(df: DataFrame, year: int, month: int) -> DataFrame:
        """R fix: In Nov/2021 Bolsa Familia the first two cols are swapped:
        'MÊS COMPETÊNCIA' and 'MÊS REFERÊNCIA'. We fix by swapping/renaming.

        Note: we normalize columns to ASCII snake_case before this runs, so we handle:
        - mes_competencia
        - mes_referencia
        """
        if year == 2021 and month == 11:
            cols = set(df.columns)
            if "mes_competencia" in cols and "mes_referencia" in cols:
                df = (
                    df.withColumnRenamed("mes_competencia", "__tmp_mes_competencia")
                    .withColumnRenamed("mes_referencia", "mes_competencia")
                    .withColumnRenamed("__tmp_mes_competencia", "mes_referencia")
                )
        return df

    def ingest_all(self) -> None:
        zip_files = self._list_zip_files()
        if not zip_files:
            raise RuntimeError(f"No zip files found in {self.paths.source_zips_dir}")

        self.spark.sql("SET spark.databricks.delta.schema.autoMerge.enabled=true")

        for idx, z in enumerate(zip_files, start=1):
            year, month = self._month_from_name(z)
            df_raw, inner = self._read_zip_csv(z)
            df_norm = self._normalize_cols(df_raw)
            df_norm = self._fix_nov_2021_header_swap(df_norm, year, month)

            origin = self._origin_from_name(z)
            df_out = (
                df_norm.withColumn("origin", F.lit(origin))
                .withColumn("ano", F.lit(year).cast("int"))
                .withColumn("mes", F.lit(month).cast("int"))
                .withColumn("competencia", F.format_string("%04d%02d", F.col("ano"), F.col("mes")))
                .withColumn("source_zip", F.lit(str(z)))
                .withColumn("source_inner", F.lit(inner))
                .withColumn("ingest_ts", F.current_timestamp())
            )

            (
                df_out.write.format("delta")
                .mode("append")
                .partitionBy("origin", "ano", "mes")
                .save(self.paths.bronze_payments_path)
            )

            print(
                f"[{idx}/{len(zip_files)}] Ingested {z.name} -> {self.paths.bronze_payments_path} "
                f"(origin={origin}, year={year}, month={month})"
            )
