"""
reconciliation/reconcile_checker.py
=====================================
All reconciliation check classes.

These checks compare SOURCE dataset vs TARGET dataset to verify
that ETL transformation logic worked correctly.

7 Check Types:
  1. RowCountChecker         — source row count vs target row count
  2. ColumnMatchChecker      — columns that should be identical in both
  3. TransformationChecker   — verify transformation rule was applied correctly
  4. AggregateMatchChecker   — SUM/AVG/MIN/MAX must match between source & target
  5. MissingRowsChecker      — rows present in source but missing from target
  6. ExtraRowsChecker        — rows in target that don't exist in source
  7. DuplicateExplosionChecker — target has more copies of a row than source (bad JOIN)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import List, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


# ==============================================================================
# ReconcileResult — one object per reconciliation check outcome
# ==============================================================================

@dataclass
class ReconcileResult:
    """
    Stores the outcome of ONE reconciliation check.

    Fields
    ------
    check_type   : e.g. "Row Count", "Column Match"
    check_name   : specific name e.g. "student_id match"
    status       : PASS | FAIL | SKIP | ERROR
    source_value : what the source had (count, sum, etc.)
    target_value : what the target had
    difference   : source_value - target_value (0 = perfect match)
    message      : plain English explanation
    """
    check_type   : str
    check_name   : str
    status       : str
    source_value : str = ""
    target_value : str = ""
    difference   : str = ""
    message      : str = ""

    @property
    def passed(self) -> bool:
        return self.status == "PASS"

    @property
    def failed(self) -> bool:
        return self.status == "FAIL"

    def to_dict(self) -> dict:
        return {
            "check_type"   : self.check_type,
            "check_name"   : self.check_name,
            "status"       : self.status,
            "source_value" : self.source_value,
            "target_value" : self.target_value,
            "difference"   : self.difference,
            "message"      : self.message,
        }


# ==============================================================================
# Base class
# ==============================================================================

class BaseReconcileChecker:
    """Shared utilities for all reconciliation checkers."""

    _SEP = "=" * 70
    _DIV = "-" * 50

    def __init__(self, source_df: DataFrame, target_df: DataFrame) -> None:
        self.src     = source_df
        self.tgt     = target_df
        self.log     = logging.getLogger(self.__class__.__name__)
        self.results : List[ReconcileResult] = []

        # Cache row counts — used by multiple checkers
        self._src_count : Optional[int] = None
        self._tgt_count : Optional[int] = None

    @property
    def src_count(self) -> int:
        if self._src_count is None:
            self._src_count = self.src.count()
        return self._src_count

    @property
    def tgt_count(self) -> int:
        if self._tgt_count is None:
            self._tgt_count = self.tgt.count()
        return self._tgt_count

    def _header(self, title: str) -> None:
        print(f"\n{self._SEP}")
        print(f"  {title}")
        print(self._SEP)

    def _sub(self, label: str) -> None:
        print(f"\n   {label}")
        print(f"   {self._DIV}")

    def _record(self, check_type: str, check_name: str, passed: bool,
                src_val: str = "", tgt_val: str = "",
                diff: str = "", msg: str = "") -> ReconcileResult:
        status = "PASS" if passed else "FAIL"
        icon   = "✓" if passed else "✗"
        result = ReconcileResult(check_type, check_name, status,
                                 src_val, tgt_val, diff, msg)
        self.results.append(result)
        if passed:
            print(f"   Result : PASS {icon}")
        else:
            print(f"   Result : FAIL {icon}  —  {msg}")
        self.log.info(f"{check_name} | {status} | src={src_val} tgt={tgt_val}")
        return result

    def _skip(self, check_type: str, check_name: str, reason: str) -> ReconcileResult:
        result = ReconcileResult(check_type, check_name, "SKIP", message=reason)
        self.results.append(result)
        print(f"   Result : SKIP  ~  {reason}")
        return result

    def _error(self, check_type: str, check_name: str, reason: str) -> ReconcileResult:
        result = ReconcileResult(check_type, check_name, "ERROR", message=reason)
        self.results.append(result)
        print(f"   Result : ERROR !  {reason}")
        self.log.error(f"{check_name} ERROR: {reason}")
        return result


# ==============================================================================
# 1. RowCountChecker
# ==============================================================================

class RowCountChecker(BaseReconcileChecker):
    """
    Compares total row count between source and target.

    In ETL, the target row count depends on your transformation:
      - FULL LOAD     : target rows = source rows  (tolerance = 0)
      - FILTERED LOAD : target rows < source rows  (expected_target set manually)
      - AGGREGATED    : target rows << source rows

    tolerance_pct: acceptable % difference. Default 0 (exact match required).
    expected_target_count: if your transformation filters rows, set this.
    """

    def __init__(self, source_df: DataFrame, target_df: DataFrame,
                 tolerance_pct: float = 0.0,
                 expected_target_count: Optional[int] = None) -> None:
        super().__init__(source_df, target_df)
        self.tolerance_pct          = tolerance_pct
        self.expected_target_count  = expected_target_count

    def run(self) -> List[ReconcileResult]:
        self._header("ROW COUNT CHECK")
        try:
            src_n = self.src_count
            tgt_n = self.tgt_count
            expected = self.expected_target_count if self.expected_target_count else src_n

            diff     = tgt_n - expected
            pct_diff = abs(diff) / expected * 100 if expected > 0 else 0.0

            self._sub("Source vs Target Row Count")
            print(f"   Source rows    : {src_n:,}")
            print(f"   Target rows    : {tgt_n:,}")
            print(f"   Expected target: {expected:,}")
            print(f"   Difference     : {diff:+,}  ({pct_diff:.2f}%)")
            print(f"   Tolerance      : {self.tolerance_pct}%")

            passed = pct_diff <= self.tolerance_pct
            self._record(
                "Row Count", "Row Count Match",
                passed,
                src_val = str(src_n),
                tgt_val = str(tgt_n),
                diff    = f"{diff:+,}",
                msg     = f"{pct_diff:.2f}% difference exceeds tolerance {self.tolerance_pct}%"
                          if not passed else ""
            )
        except Exception as exc:
            self._error("Row Count", "Row Count Match", str(exc))
        return self.results


# ==============================================================================
# 2. ColumnMatchChecker
# ==============================================================================

class ColumnMatchChecker(BaseReconcileChecker):
    """
    Checks columns that should be IDENTICAL between source and target.
    These are columns where NO transformation was applied.

    Method: joins on the key column, then compares each value pair.
    Mismatches = rows where source value != target value.
    """

    def __init__(self, source_df: DataFrame, target_df: DataFrame,
                 join_key: str, columns: List[str]) -> None:
        super().__init__(source_df, target_df)
        self.join_key = join_key
        self.columns  = columns

    def run(self) -> List[ReconcileResult]:
        self._header("COLUMN MATCH CHECK  (columns that should be identical)")
        try:
            # Join source and target on the key column
            # Rename target columns to avoid ambiguity
            tgt_renamed = self.tgt
            for col in self.columns:
                tgt_renamed = tgt_renamed.withColumnRenamed(col, f"_tgt_{col}")

            joined = self.src.join(
                tgt_renamed,
                self.src[self.join_key] == tgt_renamed[self.join_key],
                how="inner"
            )

            total_joined = joined.count()
            self._sub(f"Join key: {self.join_key}  |  Joined rows: {total_joined:,}")

            for col in self.columns:
                self._check_column(joined, col, total_joined)

        except Exception as exc:
            self._error("Column Match", str(self.columns), str(exc))
        return self.results

    def _check_column(self, joined: DataFrame, col: str, total: int) -> None:
        self._sub(f"Column : {col}")
        try:
            # Count rows where source value != target value
            mismatches = joined.filter(
                F.col(col).isNotNull() &
                F.col(f"_tgt_{col}").isNotNull() &
                (F.col(col) != F.col(f"_tgt_{col}"))
            ).count()

            print(f"   Joined rows : {total:,}  |  Mismatches : {mismatches:,}")

            if mismatches > 0:
                # Show sample mismatches
                joined.filter(F.col(col) != F.col(f"_tgt_{col}")) \
                      .select(
                          self.join_key,
                          F.col(col).alias("source"),
                          F.col(f"_tgt_{col}").alias("target")
                      ).show(5, truncate=False)

            self._record(
                "Column Match", f"{col} match",
                mismatches == 0,
                src_val = f"{total:,} rows",
                tgt_val = f"{total:,} rows",
                diff    = str(mismatches),
                msg     = f"{mismatches:,} rows have different values"
                          if mismatches > 0 else ""
            )
        except Exception as exc:
            self._error("Column Match", col, str(exc))


# ==============================================================================
# 3. TransformationChecker
# ==============================================================================

class TransformationChecker(BaseReconcileChecker):
    """
    Verifies that a specific transformation was applied correctly.

    Supported rules:
      UPPER        : target = UPPER(source)
      LOWER        : target = LOWER(source)
      TRIM         : target = TRIM(source)
      ROUND_2      : target = ROUND(source, 2)
      NOT_NULL     : source was null → target filled with default
      EXPR         : custom SQL expression
    """

    SUPPORTED_RULES = ["UPPER", "LOWER", "TRIM", "ROUND_2", "NOT_NULL", "EXPR"]

    def __init__(self, source_df: DataFrame, target_df: DataFrame,
                 join_key: str, rules: List[dict]) -> None:
        super().__init__(source_df, target_df)
        self.join_key = join_key
        self.rules    = rules

    def run(self) -> List[ReconcileResult]:
        self._header("TRANSFORMATION CHECK  (verify transform rules were applied)")
        for rule in self.rules:
            src_col  = rule.get("source_col", "")
            tgt_col  = rule.get("target_col", src_col)
            rule_type= rule.get("rule", "").upper()
            self._check_rule(src_col, tgt_col, rule_type, rule)
        return self.results

    def _check_rule(self, src_col: str, tgt_col: str,
                    rule_type: str, rule: dict) -> None:
        self._sub(f"Rule : {rule_type}  |  Source : {src_col}  →  Target : {tgt_col}")
        try:
            tgt_alias = self.tgt.withColumnRenamed(tgt_col, f"_tgt_{tgt_col}")
            joined = self.src.join(
                tgt_alias,
                self.src[self.join_key] == tgt_alias[self.join_key],
                how="inner"
            )
            total = joined.count()

            if rule_type == "UPPER":
                bad = joined.filter(
                    F.col(src_col).isNotNull() &
                    (F.col(f"_tgt_{tgt_col}") != F.upper(F.col(src_col)))
                ).count()

            elif rule_type == "LOWER":
                bad = joined.filter(
                    F.col(src_col).isNotNull() &
                    (F.col(f"_tgt_{tgt_col}") != F.lower(F.col(src_col)))
                ).count()

            elif rule_type == "TRIM":
                bad = joined.filter(
                    F.col(src_col).isNotNull() &
                    (F.col(f"_tgt_{tgt_col}") != F.trim(F.col(src_col)))
                ).count()

            elif rule_type == "ROUND_2":
                bad = joined.filter(
                    F.col(src_col).isNotNull() &
                    (F.round(F.col(src_col).cast("double"), 2) !=
                     F.col(f"_tgt_{tgt_col}").cast("double"))
                ).count()

            elif rule_type == "NOT_NULL":
                # Target should have no NULLs even where source had NULL
                default_val = rule.get("default_value", "")
                bad = joined.filter(
                    F.col(src_col).isNull() &
                    (F.col(f"_tgt_{tgt_col}") != str(default_val))
                ).count()

            elif rule_type == "EXPR":
                expr_str = rule.get("expression", "1=1")
                bad = joined.filter(~F.expr(expr_str)).count()

            else:
                self._skip("Transformation", f"{src_col}→{tgt_col}",
                           f"Unknown rule '{rule_type}'")
                return

            print(f"   Total rows checked : {total:,}  |  Rule violations : {bad:,}")
            self._record(
                "Transformation", f"{rule_type}: {src_col}→{tgt_col}",
                bad == 0,
                src_val = src_col,
                tgt_val = tgt_col,
                diff    = str(bad),
                msg     = f"{bad:,} rows where {rule_type} was not applied correctly"
                          if bad > 0 else ""
            )
        except Exception as exc:
            self._error("Transformation", f"{src_col}→{tgt_col}", str(exc))


# ==============================================================================
# 4. AggregateMatchChecker
# ==============================================================================

class AggregateMatchChecker(BaseReconcileChecker):
    """
    Compares SUM, AVG, MIN, MAX between source and target for numeric columns.
    These should match exactly for columns where values are not transformed.
    A tolerance_pct can be set for acceptable rounding differences.
    """

    def __init__(self, source_df: DataFrame, target_df: DataFrame,
                 columns: List[str],
                 functions: List[str] = None,
                 tolerance_pct: float = 0.01) -> None:
        super().__init__(source_df, target_df)
        self.columns      = columns
        self.functions    = [f.upper() for f in (functions or ["SUM", "AVG", "MIN", "MAX"])]
        self.tolerance_pct = tolerance_pct

    def run(self) -> List[ReconcileResult]:
        self._header(f"AGGREGATE MATCH CHECK  (tolerance : {self.tolerance_pct}%)")
        for col in self.columns:
            self._sub(f"Column : {col}")
            for func in self.functions:
                self._check_aggregate(col, func)
        return self.results

    def _check_aggregate(self, col: str, func: str) -> None:
        try:
            # Cast to double for consistent comparison
            src_val = self.src.agg(
                getattr(F, func.lower())(F.col(col).cast("double"))
            ).collect()[0][0]

            tgt_val = self.tgt.agg(
                getattr(F, func.lower())(F.col(col).cast("double"))
            ).collect()[0][0]

            if src_val is None and tgt_val is None:
                self._skip("Aggregate Match", f"{func}({col})",
                           "Both source and target are NULL")
                return

            src_val  = round(float(src_val or 0), 4)
            tgt_val  = round(float(tgt_val or 0), 4)
            diff     = abs(src_val - tgt_val)
            pct_diff = (diff / abs(src_val) * 100) if src_val != 0 else 0.0

            print(f"   {func}({col}) | Source={src_val:,.4f} | Target={tgt_val:,.4f} | "
                  f"Diff={diff:,.4f} ({pct_diff:.4f}%)")

            passed = pct_diff <= self.tolerance_pct
            self._record(
                "Aggregate Match", f"{func}({col})",
                passed,
                src_val = f"{src_val:,.4f}",
                tgt_val = f"{tgt_val:,.4f}",
                diff    = f"{diff:,.4f} ({pct_diff:.4f}%)",
                msg     = f"Difference {pct_diff:.4f}% exceeds tolerance {self.tolerance_pct}%"
                          if not passed else ""
            )
        except Exception as exc:
            self._error("Aggregate Match", f"{func}({col})", str(exc))


# ==============================================================================
# 5. MissingRowsChecker
# ==============================================================================

class MissingRowsChecker(BaseReconcileChecker):
    """
    Finds rows that exist in SOURCE but are MISSING from TARGET.

    These are rows that were silently dropped during the ETL.
    Uses a LEFT ANTI JOIN:
      Source LEFT ANTI JOIN Target ON join_key
      → Returns rows in source with NO matching row in target
    """

    def __init__(self, source_df: DataFrame, target_df: DataFrame,
                 join_key: str) -> None:
        super().__init__(source_df, target_df)
        self.join_key = join_key

    def run(self) -> List[ReconcileResult]:
        self._header(f"MISSING ROWS CHECK  (in source but not in target)")
        self._sub(f"Join key : {self.join_key}")
        try:
            src_keys = self.src.select(self.join_key)
            tgt_keys = self.tgt.select(self.join_key)

            # LEFT ANTI JOIN: gives rows in src that have NO match in tgt
            missing = src_keys.join(tgt_keys, on=self.join_key, how="left_anti")
            missing_count = missing.count()

            print(f"   Source rows : {self.src_count:,}")
            print(f"   Target rows : {self.tgt_count:,}")
            print(f"   Missing rows (in source, not in target) : {missing_count:,}")

            if missing_count > 0:
                print(f"\n   Sample missing {self.join_key} values:")
                missing.show(10, truncate=False)

            self._record(
                "Missing Rows", f"Missing in target",
                missing_count == 0,
                src_val = str(self.src_count),
                tgt_val = str(self.tgt_count),
                diff    = str(missing_count),
                msg     = f"{missing_count:,} rows from source are missing in target"
                          if missing_count > 0 else ""
            )
        except Exception as exc:
            self._error("Missing Rows", "Missing in target", str(exc))
        return self.results


# ==============================================================================
# 6. ExtraRowsChecker
# ==============================================================================

class ExtraRowsChecker(BaseReconcileChecker):
    """
    Finds rows in TARGET that do NOT exist in SOURCE.

    These are phantom rows — created by bad inserts, wrong joins,
    or incorrect transformation logic.
    Uses a LEFT ANTI JOIN in the other direction:
      Target LEFT ANTI JOIN Source ON join_key
    """

    def __init__(self, source_df: DataFrame, target_df: DataFrame,
                 join_key: str) -> None:
        super().__init__(source_df, target_df)
        self.join_key = join_key

    def run(self) -> List[ReconcileResult]:
        self._header(f"EXTRA ROWS CHECK  (in target but not in source)")
        self._sub(f"Join key : {self.join_key}")
        try:
            src_keys = self.src.select(self.join_key)
            tgt_keys = self.tgt.select(self.join_key)

            # LEFT ANTI JOIN reversed: rows in target with NO match in source
            extra = tgt_keys.join(src_keys, on=self.join_key, how="left_anti")
            extra_count = extra.count()

            print(f"   Source rows : {self.src_count:,}")
            print(f"   Target rows : {self.tgt_count:,}")
            print(f"   Extra rows  (in target, not in source) : {extra_count:,}")

            if extra_count > 0:
                print(f"\n   Sample extra {self.join_key} values:")
                extra.show(10, truncate=False)

            self._record(
                "Extra Rows", "Extra in target",
                extra_count == 0,
                src_val = str(self.src_count),
                tgt_val = str(self.tgt_count),
                diff    = str(extra_count),
                msg     = f"{extra_count:,} rows in target have no matching source row"
                          if extra_count > 0 else ""
            )
        except Exception as exc:
            self._error("Extra Rows", "Extra in target", str(exc))
        return self.results


# ==============================================================================
# 7. DuplicateExplosionChecker
# ==============================================================================

class DuplicateExplosionChecker(BaseReconcileChecker):
    """
    Detects when ETL creates MORE copies of a row than exist in source.

    Classic symptom of a bad JOIN in the transformation — one source row
    becomes 3 or 4 target rows.

    Example:
      Source: student_id=101 appears 1 time
      Target: student_id=101 appears 4 times  ← JOIN exploded!
    """

    def __init__(self, source_df: DataFrame, target_df: DataFrame,
                 join_key: str) -> None:
        super().__init__(source_df, target_df)
        self.join_key = join_key

    def run(self) -> List[ReconcileResult]:
        self._header(f"DUPLICATE EXPLOSION CHECK  (JOIN explosion detection)")
        self._sub(f"Join key : {self.join_key}")
        try:
            # Count occurrences of each key in source and target
            src_counts = self.src.groupBy(self.join_key).count() \
                             .withColumnRenamed("count", "src_cnt")
            tgt_counts = self.tgt.groupBy(self.join_key).count() \
                             .withColumnRenamed("count", "tgt_cnt")

            # Join the counts and find where target count > source count
            compared = src_counts.join(tgt_counts, on=self.join_key, how="inner")
            exploded = compared.filter(F.col("tgt_cnt") > F.col("src_cnt"))
            explosion_count = exploded.count()

            print(f"   Keys in source : {src_counts.count():,}")
            print(f"   Keys in target : {tgt_counts.count():,}")
            print(f"   Exploded keys  : {explosion_count:,}")

            if explosion_count > 0:
                print(f"\n   Sample exploded rows:")
                exploded.show(10, truncate=False)

            self._record(
                "Duplicate Explosion", "No JOIN explosion",
                explosion_count == 0,
                src_val = f"{src_counts.count():,} distinct keys",
                tgt_val = f"{tgt_counts.count():,} distinct keys",
                diff    = str(explosion_count),
                msg     = f"{explosion_count:,} keys have more rows in target than source"
                          if explosion_count > 0 else ""
            )
        except Exception as exc:
            self._error("Duplicate Explosion", "No JOIN explosion", str(exc))
        return self.results
