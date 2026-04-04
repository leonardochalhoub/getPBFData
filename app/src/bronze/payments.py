from __future__ import annotations

import re
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple

from pyspark.sql import DataFrame, SparkSession, functions as F, types as T


_MONTH_RE = re.compile(r"(?P<year>20\d{2})[_-]?(?P<month>0[1-9]|1[0-2])", re.IGNORECASE)
_SOURCE_RE = re.compile(r"^(?P<src>PBF|AUX_BR|AUX|NBF)[_-]", re.IGNORECASE)


@dataclass(frozen=True)
class BronzePaths:
    source_zips_dir: Path
    bronze_root: Path

    @staticmethod
    def for_lakehouse(source_zips_dir: Path, lakehouse_root: Path) -> "BronzePaths":
        return BronzePaths(source_zips_dir=source_zips_dir, bronze_root=lakehouse_root / "bronze")

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
    """Ingests monthly ZIP files into a Delta Lake Bronze table.

    Strategy:
    - Extracts the inner CSV to a temporary folder on disk (one ZIP at a time), reads it with Spark,
      then deletes the extracted file(s) after a successful Delta write.
    - Normalizes column names to Delta-friendly ASCII snake_case.
    - Fixes Nov/2021 Bolsa Família header swap issue (mes_competencia/mes_referencia).
    - Adds metadata:
        - ano, mes, competencia (yyyymm inferred from filename)
        - origin (AUX/PBF/NBF inferred from filename prefix)
        - source_zip, source_inner, ingest_ts
    - Writes using partitioned Delta layout: partitionBy(origin, ano, mes). Ingestion is idempotent
      per partition via dynamic partition overwrite at batch scope.

    Special rule:
    - For 2021-11, there are two program files (PBF and AUX). We create an extra origin
      "PBF_AUX_SUM" for that month containing the summed values (numeric columns summed,
      non-numeric columns taken as first non-null). This matches the legacy behavior where
      2021-11 is treated as a single combined dataset.
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
        """Extract inner CSV to a temp folder, read with Spark, then delete the extracted CSV.

        Why:
        - Reading huge CSVs from an in-memory RDD creates giant Spark tasks and can deadlock.
        - This keeps disk usage bounded: we extract one CSV at a time then remove it after the
          Spark read has loaded it.
        """
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

        # IMPORTANT: do NOT delete here.
        # Spark is lazy and will read the file later during the write action.
        # We delete the extracted file only after the downstream write succeeds.
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

    def ingest_all(
        self,
        batch_size: int = 12,
        *,
        origin_allow: Optional[set[str]] = None,
        min_year_month: Optional[tuple[int, int]] = None,
    ) -> None:
        zip_files = self._list_zip_files()
        if not zip_files:
            raise RuntimeError(f"No zip files found in {self.paths.source_zips_dir}")

        self.spark.sql("SET spark.databricks.delta.schema.autoMerge.enabled=true")

        # Group zips by (year, month) so we can batch multiple months into a single Spark job.
        month_groups: dict[tuple[int, int], list[Path]] = {}
        for z in zip_files:
            origin = self._origin_from_name(z).upper()
            if origin_allow is not None and origin not in origin_allow:
                continue

            ym = self._month_from_name(z)
            if min_year_month is not None:
                min_y, min_m = min_year_month
                y, m = ym
                if (y, m) < (min_y, min_m):
                    continue

            month_groups.setdefault(ym, []).append(z)

        month_items = sorted(month_groups.items())
        total_months = len(month_items)

        # Use dynamic partition overwrite so we can write many partitions in one shot,
        # while remaining idempotent for the partitions included in the batch.
        self.spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

        def chunked(seq: list[tuple[tuple[int, int], list[Path]]], n: int):
            for i in range(0, len(seq), n):
                yield seq[i : i + n]

        for b_idx, batch in enumerate(chunked(month_items, batch_size), start=1):
            dfs: list[DataFrame] = []

            for (year, month), files in batch:
                month_dfs: list[DataFrame] = []

                for z in sorted(files):
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

                    dfs.append(df_out)
                    month_dfs.append(df_out)

                    # After the batch write action completes, Spark has consumed the file; safe to delete now.
                    # We still delete now, but only after forcing the dataframe to materialize for this file.
                    # (We do that by caching+count below at batch level.)
                    tmp_csv = self.paths.bronze_root / "_tmp_extract" / z.stem / Path(inner).name
                    tmp_dir = tmp_csv.parent

                    # record tmp for later cleanup
                    df_out = df_out.withColumn("_tmp_csv_path", F.lit(str(tmp_csv))).withColumn(
                        "_tmp_dir_path", F.lit(str(tmp_dir))
                    )
                    dfs[-1] = df_out
                    month_dfs[-1] = df_out

                # Special month: 2021-11 sum across PBF + AUX into an extra origin
                if year == 2021 and month == 11:
                    combined = None
                    for df in month_dfs:
                        combined = df if combined is None else combined.unionByName(df, allowMissingColumns=True)

                    if combined is not None:
                        meta_cols = {
                            "origin",
                            "ano",
                            "mes",
                            "competencia",
                            "source_zip",
                            "source_inner",
                            "ingest_ts",
                            "_tmp_csv_path",
                            "_tmp_dir_path",
                        }
                        numeric_cols = [
                            c
                            for c, t in combined.dtypes
                            if c not in meta_cols and t in ("int", "bigint", "double", "float", "decimal")
                        ]
                        other_cols = [c for c in combined.columns if c not in meta_cols and c not in numeric_cols]

                        agg_exprs = []
                        for c in numeric_cols:
                            agg_exprs.append(F.sum(F.col(c)).alias(c))
                        for c in other_cols:
                            agg_exprs.append(F.first(F.col(c), ignorenulls=True).alias(c))

                        summed = (
                            combined.filter(F.col("origin").isin(["PBF", "AUX"]))
                            .groupBy("ano", "mes", "competencia")
                            .agg(*agg_exprs)
                            .withColumn("origin", F.lit("PBF_AUX_SUM"))
                            .withColumn("source_zip", F.lit("MULTI"))
                            .withColumn("source_inner", F.lit("MULTI"))
                            .withColumn("ingest_ts", F.current_timestamp())
                            .withColumn("_tmp_csv_path", F.lit(None).cast("string"))
                            .withColumn("_tmp_dir_path", F.lit(None).cast("string"))
                        )
                        dfs.append(summed)

            if not dfs:
                continue

            batch_df = dfs[0]
            for d in dfs[1:]:
                batch_df = batch_df.unionByName(d, allowMissingColumns=True)

            # DO NOT persist/cache the full batch:
            # It can be huge and will spill to disk (the MemoryStore warnings you're seeing).
            # We rely on the fact that the Delta write is the action that triggers the read.
            # Cleanup is done after the write, and it is best-effort.
            (
                batch_df.drop("_tmp_csv_path", "_tmp_dir_path")
                .write.format("delta")
                .mode("overwrite")
                .partitionBy("origin", "ano", "mes")
                .save(self.paths.bronze_payments_path)
            )

            # Cleanup extracted CSVs after successful write.
            # Collecting distinct paths is small (1 per input zip).
            tmp_rows = (
                batch_df.select("_tmp_csv_path", "_tmp_dir_path")
                .where(F.col("_tmp_csv_path").isNotNull())
                .distinct()
                .collect()
            )
            for r in tmp_rows:
                tmp_csv = Path(r["_tmp_csv_path"])
                tmp_dir = Path(r["_tmp_dir_path"])
                try:
                    tmp_csv.unlink(missing_ok=True)
                except Exception:
                    pass
                try:
                    tmp_dir.rmdir()
                except Exception:
                    pass

            done = min(b_idx * batch_size, total_months)
            print(f"[batch {b_idx}] wrote {done}/{total_months} months -> {self.paths.bronze_payments_path}")
