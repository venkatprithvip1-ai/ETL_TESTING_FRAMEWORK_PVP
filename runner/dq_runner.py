"""
runner/dq_runner.py
===================
Core runner components:

  SparkSessionManager  — start / stop Spark
  DataReader           — reads CSV, Parquet, or Delta Lake
  DQConfigLoader       — loads rules from YAML file
  DQRunner             — parallel execution of all checkers + summary
"""

from __future__ import annotations

import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Any, Optional

import yaml
from pyspark.sql import DataFrame, SparkSession

import checkers
from checkers.all_checkers import (
    AllowedValuesChecker, CaseChecker, CompletenessRatioChecker,
    ConsistencyChecker, CrossColumnChecker, DataTypeChecker,
    DefaultValueChecker, FreshnessChecker, LengthChecker,
    MandatoryFieldChecker, OutlierChecker, PatternChecker,
    PrecisionScaleChecker, PrimaryKeyChecker, ReferentialIntegrityChecker,
    SpecialCharacterChecker, UniquenessChecker, ValueRangeChecker,
    WhitespaceChecker,
)
from core.base import BaseChecker, CheckResult
from core.exceptions import (
    DQConfigError, DQDataReadError, DQSparkError,
)


# ==============================================================================
# SparkSessionManager
# ==============================================================================

class SparkSessionManager:
    """Manages the Spark session lifecycle."""

    def __init__(self, app_name: str = "ETL_DQ_v6", master: str = "local[*]") -> None:
        self.app_name = app_name
        self.master   = master
        self.spark    : Optional[SparkSession] = None
        self.log      = logging.getLogger(self.__class__.__name__)

    def start(self) -> SparkSession:
        try:
            self.log.info(f"Starting Spark | app={self.app_name} | master={self.master}")
            builder = (
                SparkSession.builder
                .appName(self.app_name)
                .master(self.master)
                .config("spark.ui.showConsoleProgress", "false")
                .config("spark.sql.adaptive.enabled", "true")       # better query plans
                .config("spark.sql.shuffle.partitions", "8")        # tuned for local mode
            )

            # Delta Lake support — only if delta-spark is installed
            try:
                builder = builder \
                    .config("spark.sql.extensions",
                            "io.delta.sql.DeltaSparkSessionExtension") \
                    .config("spark.sql.catalog.spark_catalog",
                            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            except Exception:
                self.log.info("Delta Lake config skipped — delta-spark not installed")

            self.spark = builder.getOrCreate()
            self.spark.sparkContext.setLogLevel("ERROR")
            self.log.info("Spark started successfully")
            return self.spark
        except Exception as exc:
            raise DQSparkError(f"Failed to start Spark: {exc}") from exc

    def stop(self) -> None:
        if self.spark:
            try:
                self.spark.stop()
                print("\n  Spark session closed.")
                self.log.info("Spark stopped")
            except Exception as exc:
                self.log.error(f"Error stopping Spark: {exc}")


# ==============================================================================
# DataReader — supports CSV, Parquet, Delta Lake
# ==============================================================================

class DataReader:
    """
    Reads input data into a Spark DataFrame.
    Supports: CSV, Parquet, Delta Lake.
    Always reads as strings (inferSchema=False) so DataTypeChecker works correctly.
    """

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark
        self.log   = logging.getLogger(self.__class__.__name__)

    def read(self, file_path: str, fmt: str = "csv") -> DataFrame:
        """
        Parameters
        ----------
        file_path : path to data file or Delta table path
        fmt       : "csv" | "parquet" | "delta"
        """
        if not os.path.exists(file_path):
            raise DQDataReadError(
                f"File not found: '{file_path}'. "
                "Run: python data/generate_sample_data.py"
            )

        self.log.info(f"Reading {fmt.upper()} | path={file_path}")
        try:
            if fmt == "csv":
                df = self.spark.read.csv(file_path, header=True, inferSchema=False)

            elif fmt == "parquet":
                # Read parquet but cast all columns to string for consistent checking
                df = self.spark.read.parquet(file_path)
                for c in df.columns:
                    df = df.withColumn(c, df[c].cast("string"))

            elif fmt == "delta":
                df = self.spark.read.format("delta").load(file_path)
                for c in df.columns:
                    df = df.withColumn(c, df[c].cast("string"))
            else:
                raise DQDataReadError(f"Unsupported format: '{fmt}'. Use csv/parquet/delta.")

            self.log.info(f"Loaded | rows={df.count()} | cols={df.columns}")
            return df

        except DQDataReadError:
            raise
        except Exception as exc:
            raise DQDataReadError(f"Failed to read '{file_path}': {exc}") from exc


# ==============================================================================
# DQConfigLoader — reads YAML rule files
# ==============================================================================

class DQConfigLoader:
    """
    Loads DQ rules from a YAML config file.
    This means you NEVER need to edit Python code to change check settings.
    Just edit the YAML file and re-run.
    """

    def __init__(self, yaml_path: str) -> None:
        self.yaml_path = yaml_path
        self.log       = logging.getLogger(self.__class__.__name__)

    def load(self) -> dict:
        """Loads and returns the YAML config as a Python dict."""
        if not os.path.exists(self.yaml_path):
            raise DQConfigError(f"YAML config not found: '{self.yaml_path}'")
        try:
            with open(self.yaml_path, "r") as f:
                config = yaml.safe_load(f)
            self.log.info(f"Config loaded | path={self.yaml_path}")
            return config
        except yaml.YAMLError as exc:
            raise DQConfigError(f"Invalid YAML in '{self.yaml_path}': {exc}") from exc


# ==============================================================================
# DQRunner — parallel checker execution + summary
# ==============================================================================

class DQRunner:
    """
    Orchestrates all DQ checks for ONE dataset.

    NEW in v6: PARALLEL EXECUTION
    → Uses ThreadPoolExecutor to run multiple checkers concurrently.
    → On a 4-core Mac, this can cut total runtime by 40–60%.
    → The max_workers parameter controls how many checkers run in parallel.
      Default: 4 (safe for local Spark mode).

    NOTE: Spark DataFrames are thread-safe for READ operations.
    Each checker creates its own Spark query plan independently.
    """

    def __init__(
        self,
        df           : DataFrame,
        spark        : SparkSession,
        config       : dict,
        dataset_name : str = "dataset",
        max_workers  : int = 1,
    ) -> None:
        self.df           = df
        self.spark        = spark
        self.config       = config
        self.dataset_name = dataset_name
        self.max_workers  = max_workers
        self.log          = logging.getLogger(self.__class__.__name__)
        self.all_results  : list[CheckResult] = []

    def _build_checkers(self) -> list[BaseChecker]:
        """
        Reads the YAML config and instantiates the appropriate checker objects.
        Returns a list of checker instances ready to call .run() on.
        """
        cfg      = self.config.get("checks", {})
        dn       = self.dataset_name
        checkers = []

        # Build reference DataFrames for RI checks
        ri_refs = {}
        for ri_rule in cfg.get("referential_integrity", []):
            ref_col    = ri_rule.get("ref_col", "")
            ref_values = ri_rule.get("ref_values", [])
            if ref_col and ref_values:
                ri_refs[ri_rule["fk_col"]] = self.spark.createDataFrame(
                    [(v,) for v in ref_values], [ref_col]
                )

        # ── Instantiate checkers based on what's in the YAML ─────────────────

        if pk := cfg.get("primary_key"):
            checkers.append(PrimaryKeyChecker(self.df, pk["column"], dn))

        if mf := cfg.get("mandatory_fields"):
            checkers.append(MandatoryFieldChecker(self.df, mf["columns"], dn))

        if dt := cfg.get("data_types"):
            checkers.append(DataTypeChecker(self.df, dt, dataset_name=dn))

        if pa := cfg.get("patterns"):
            checkers.append(PatternChecker(self.df, pa, dn))

        if le := cfg.get("lengths"):
            checkers.append(LengthChecker(self.df, le, dn))

        if vr := cfg.get("value_ranges"):
            checkers.append(ValueRangeChecker(self.df, vr, dn))

        if av := cfg.get("allowed_values"):
            checkers.append(AllowedValuesChecker(self.df, av, dn))

        if uq := cfg.get("uniqueness"):
            checkers.append(UniquenessChecker(self.df, uq["columns"], dn))

        if co := cfg.get("consistency"):
            checkers.append(ConsistencyChecker(self.df, co, dn))

        for ri_rule in cfg.get("referential_integrity", []):
            fk_col  = ri_rule["fk_col"]
            ref_col = ri_rule["ref_col"]
            if fk_col in ri_refs:
                checkers.append(ReferentialIntegrityChecker(
                    self.df, fk_col, ri_refs[fk_col], ref_col, dn
                ))

        if ws := cfg.get("whitespace"):
            checkers.append(WhitespaceChecker(self.df, ws["columns"], dn))

        if ps := cfg.get("precision"):
            checkers.append(PrecisionScaleChecker(self.df, ps, dn))

        if ca := cfg.get("case"):
            checkers.append(CaseChecker(self.df, ca, dn))

        if sc := cfg.get("special_chars"):
            checkers.append(SpecialCharacterChecker(self.df, sc["columns"], dataset_name=dn))

        if ot := cfg.get("outliers"):
            checkers.append(OutlierChecker(self.df, ot["columns"], dataset_name=dn))

        if dv := cfg.get("default_values"):
            checkers.append(DefaultValueChecker(self.df, dv, dn))

        if cc := cfg.get("cross_column"):
            checkers.append(CrossColumnChecker(self.df, cc, dn))

        if cr := cfg.get("completeness"):
            checkers.append(CompletenessRatioChecker(
                self.df, cr["columns"], cr.get("min_pct", 90.0), dn
            ))

        if fr := cfg.get("freshness"):
            checkers.append(FreshnessChecker(
                self.df, fr["column"], fr.get("max_days_old", 30),
                fr.get("date_fmt", "yyyy-MM-dd"), dn
            ))

        return checkers


    def run_all(self) -> list[CheckResult]:
        checkers = self._build_checkers()
        self.log.info(f"[{self.dataset_name}] Running {len(checkers)} checkers sequentially")

    # Run checkers one by one in the main thread.
    # ThreadPoolExecutor was removed because Spark's internal state
    # is not thread-safe — parallel threads cause random crashes (ERROR results)
    # and scrambled print output.
    
        for checker in checkers:
            checker_name = checker.__class__.__name__
            try:
                results = checker.run()
                self.all_results.extend(results)
            except Exception as exc:
                self.log.error(f"[{self.dataset_name}] {checker_name} CRASHED: {exc}")
                self.all_results.append(
                    CheckResult(checker_name, "unknown", "ERROR",
                            message=str(exc), dataset=self.dataset_name)
                )

        self.log.info(f"[{self.dataset_name}] All checks completed — {len(self.all_results)} results")
        return self.all_results    

        # ── PARALLEL EXECUTION ────────────────────────────────────────────────
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all checkers to the thread pool
            future_to_checker = {
                executor.submit(checker.run): checker.__class__.__name__
                for checker in checkers
            }

            # Collect results as each checker finishes
            for future in as_completed(future_to_checker):
                checker_name = future_to_checker[future]
                try:
                    results = future.result()
                    self.all_results.extend(results)
                except Exception as exc:
                    self.log.error(f"[{self.dataset_name}] {checker_name} CRASHED: {exc}")
                    self.all_results.append(
                        CheckResult(checker_name, "unknown", "ERROR",
                                    message=str(exc), dataset=self.dataset_name)
                    )

        self.log.info(f"[{self.dataset_name}] All checks completed — {len(self.all_results)} results")
        return self.all_results

    def print_summary(self) -> None:
        """Prints the PASS/FAIL summary table to the console."""
        results = self.all_results
        n_pass  = sum(1 for r in results if r.status == "PASS")
        n_fail  = sum(1 for r in results if r.status == "FAIL")
        n_skip  = sum(1 for r in results if r.status == "SKIP")
        n_err   = sum(1 for r in results if r.status == "ERROR")

        sep = "=" * 90
        print(f"\n{sep}")
        print(f"  SUMMARY — {self.dataset_name}")
        print(sep)
        print(f"  {'CHECK':<30} {'COLUMN':<28} {'STATUS':<8} {'VIOLATIONS':>10}")
        print(f"  {'-'*30} {'-'*28} {'-'*8} {'-'*10}")
        for r in sorted(results, key=lambda x: (x.status != "FAIL", x.check_name)):
            icon = {"PASS": "✓", "FAIL": "✗", "SKIP": "~", "ERROR": "!"}.get(r.status, "?")
            print(f"  {r.check_name:<30} {r.column:<28} {r.status} {icon:<3} {r.violations:>10}")
        print(f"\n  TOTAL={len(results)}  ✓PASS={n_pass}  ✗FAIL={n_fail}  ~SKIP={n_skip}  !ERROR={n_err}")
        print(sep)

    @property
    def fail_count(self) -> int:
        return sum(1 for r in self.all_results if r.status == "FAIL")
