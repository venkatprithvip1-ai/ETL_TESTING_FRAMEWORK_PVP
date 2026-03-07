"""
checkers/all_checkers.py
========================
All 19 Data Quality checker classes.
Each inherits from BaseChecker (core/base.py).
"""

from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from core.base import BaseChecker, CheckResult
from core.exceptions import DQColumnError, DQConfigError


# ── 1. PrimaryKeyChecker ──────────────────────────────────────────────────────

class PrimaryKeyChecker(BaseChecker):
    """Checks PK column: zero NULLs and zero duplicates."""

    def __init__(self, df: DataFrame, pk_column: str, dataset_name: str = "") -> None:
        super().__init__(df, dataset_name)
        self.pk_column = pk_column

    # AFTER — catches ALL exceptions inside run(), never escapes to outer handler
    def run(self) -> list[CheckResult]:
        try:
            self._validate_columns(self.pk_column)
            self._header(f"PRIMARY KEY CHECK : {self.pk_column}")
            self._null_check()
            self._duplicate_check()
        except DQColumnError as e:
            self._error("PK Check", self.pk_column, str(e))
        except Exception as e:
            self._error("PK Check", self.pk_column, f"Unexpected: {e}")
        return self.results

    def _null_check(self) -> None:
        self._sub_header(f"1. NULL CHECK — {self.pk_column}")
        try:
            null_count = self.df.filter(F.col(self.pk_column).isNull()).count()
            print(f"   Total : {self.total}  |  NULLs : {null_count}")
            if null_count > 0:
                self.df.filter(F.col(self.pk_column).isNull()).show(5, truncate=False)
            self._record("PK Null", self.pk_column, null_count == 0, null_count)
        except Exception as e:
            self._error("PK Null", self.pk_column, str(e))

    def _duplicate_check(self) -> None:
        self._sub_header(f"2. DUPLICATE CHECK — {self.pk_column}")
        try:
            unique  = self.df.select(self.pk_column).distinct().count()
            dup_cnt = self.total - unique
            print(f"   Total : {self.total}  |  Unique : {unique}  |  Duplicates : {dup_cnt}")
            if dup_cnt > 0:
                self.df.groupBy(self.pk_column).count().filter(F.col("count") > 1).show(5, truncate=False)
            self._record("PK Duplicate", self.pk_column, dup_cnt == 0, dup_cnt)
        except Exception as e:
            self._error("PK Duplicate", self.pk_column, str(e))


# ── 2. MandatoryFieldChecker ──────────────────────────────────────────────────

class MandatoryFieldChecker(BaseChecker):
    """Checks required columns have zero NULL values."""

    def __init__(self, df: DataFrame, columns: list[str], dataset_name: str = "") -> None:
        super().__init__(df, dataset_name)
        self.columns = columns

    def run(self) -> list[CheckResult]:
        try:
            self._validate_columns(*self.columns)
        except DQColumnError as e:
            return [self._error("Mandatory", str(self.columns), str(e))]

        self._header("MANDATORY FIELD CHECK")
        for col in self.columns:
            self._sub_header(f"Column : {col}")
            try:
                null_cnt = self.df.filter(F.col(col).isNull()).count()
                print(f"   Total : {self.total}  |  NULLs : {null_cnt}")
                if null_cnt > 0:
                    self.df.filter(F.col(col).isNull()).show(5, truncate=False)
                self._record("Mandatory Field", col, null_cnt == 0, null_cnt)
            except Exception as e:
                self._error("Mandatory Field", col, str(e))
        return self.results


# ── 3. DataTypeChecker ────────────────────────────────────────────────────────

class DataTypeChecker(BaseChecker):
    """Validates column values can be cast to the expected type."""

    _VALID_TYPES = {"string", "integer", "decimal", "date", "boolean"}

    def __init__(self, df: DataFrame, type_rules: dict[str, str],
                 date_fmt: str = "yyyy-MM-dd", dataset_name: str = "") -> None:
        super().__init__(df, dataset_name)
        self.type_rules = type_rules
        self.date_fmt   = date_fmt

    def run(self) -> list[CheckResult]:
        try:
            self._validate_columns(*self.type_rules.keys())
        except DQColumnError as e:
            return [self._error("Data Type", "multiple", str(e))]

        self._header("DATA TYPE CHECK")
        for col, expected in self.type_rules.items():
            self._check_column(col, expected)
        return self.results

    def _check_column(self, col: str, expected: str) -> None:
        self._sub_header(f"Column : {col}  |  Expected : {expected}")
        try:
            safe = self._safe_col_name(col)
            not_null = F.col(col).isNotNull() & (F.trim(F.col(col)) != "")

            if expected == "string":
                self._record("Data Type", col, True)
                return

            if expected == "integer":
                bad = self.df.filter(not_null & F.expr(f"try_cast(`{safe}` AS INT)").isNull()).count()
            elif expected == "decimal":
                bad = self.df.filter(not_null & F.expr(f"try_cast(`{safe}` AS DOUBLE)").isNull()).count()
            elif expected == "date":
                bad = self.df.filter(not_null & F.to_date(F.col(col), self.date_fmt).isNull()).count()
            elif expected == "boolean":
                bad = self.df.filter(not_null & (~F.lower(F.col(col)).isin(["true", "false", "1", "0"]))).count()
            else:
                self._skip("Data Type", col, f"Unknown type '{expected}'")
                return

            if bad > 0:
                bad_filter = not_null & F.expr(f"try_cast(`{safe}` AS DOUBLE)").isNull() if expected == "decimal" \
                    else not_null & F.to_date(F.col(col), self.date_fmt).isNull() if expected == "date" \
                    else not_null & F.expr(f"try_cast(`{safe}` AS INT)").isNull()
                self.df.filter(bad_filter).select(col).show(5, truncate=False)
            self._record("Data Type", col, bad == 0, bad)
        except Exception as e:
            self._error("Data Type", col, str(e))


# ── 4. PatternChecker ─────────────────────────────────────────────────────────

class PatternChecker(BaseChecker):
    """Validates column values match a regex pattern."""

    def __init__(self, df: DataFrame, pattern_rules: dict[str, str], dataset_name: str = "") -> None:
        super().__init__(df, dataset_name)
        self.pattern_rules = pattern_rules

    def run(self) -> list[CheckResult]:
        try:
            self._validate_columns(*self.pattern_rules.keys())
        except DQColumnError as e:
            return [self._error("Pattern", "multiple", str(e))]

        self._header("PATTERN / FORMAT CHECK")
        for col, pattern in self.pattern_rules.items():
            self._sub_header(f"Column : {col}  |  Pattern : {pattern}")
            try:
                bad = self.df.filter(
                    F.col(col).isNotNull() & (F.trim(F.col(col)) != "") &
                    (~F.col(col).rlike(pattern))
                ).count()
                if bad > 0:
                    self.df.filter(F.col(col).isNotNull() & (~F.col(col).rlike(pattern))).select(col).show(5, truncate=False)
                self._record("Pattern", col, bad == 0, bad)
            except Exception as e:
                self._error("Pattern", col, str(e))
        return self.results


# ── 5. LengthChecker ──────────────────────────────────────────────────────────

class LengthChecker(BaseChecker):
    """Checks string column values are within min/max character length."""

    def __init__(self, df: DataFrame, length_rules: dict[str, dict], dataset_name: str = "") -> None:
        super().__init__(df, dataset_name)
        self.length_rules = length_rules

    def run(self) -> list[CheckResult]:
        try:
            self._validate_columns(*self.length_rules.keys())
        except DQColumnError as e:
            return [self._error("Length", "multiple", str(e))]

        self._header("LENGTH CHECK")
        for col, rules in self.length_rules.items():
            min_l = rules.get("min", 0)
            max_l = rules.get("max", 99_999)
            try:
                actual_max = self.df.agg(F.max(F.length(F.col(col)))).collect()[0][0]
                self._sub_header(f"Column : {col}  |  Allowed : {min_l}–{max_l}  |  Actual max : {actual_max}")
                bad = self.df.filter(
                    F.col(col).isNotNull() &
                    ((F.length(F.col(col)) < min_l) | (F.length(F.col(col)) > max_l))
                ).count()
                self._record("Length", col, bad == 0, bad)
            except Exception as e:
                self._error("Length", col, str(e))
        return self.results


# ── 6. ValueRangeChecker ──────────────────────────────────────────────────────

class ValueRangeChecker(BaseChecker):
    """Checks numeric values are within allowed min/max range."""

    def __init__(self, df: DataFrame, range_rules: dict[str, dict], dataset_name: str = "") -> None:
        super().__init__(df, dataset_name)
        self.range_rules = range_rules

    def run(self) -> list[CheckResult]:
        try:
            self._validate_columns(*self.range_rules.keys())
        except DQColumnError as e:
            return [self._error("Value Range", "multiple", str(e))]

        self._header("VALUE RANGE CHECK")
        for col, rules in self.range_rules.items():
            min_v, max_v = rules.get("min"), rules.get("max")
            if min_v is None or max_v is None:
                self._skip("Value Range", col, "min or max not defined")
                continue
            self._sub_header(f"Column : {col}  |  Range : {min_v} to {max_v}")
            try:
                safe   = self._safe_col_name(col)
                casted = self.df.withColumn("_val", F.expr(f"try_cast(`{safe}` AS DOUBLE)"))
                bad    = casted.filter(
                    F.col("_val").isNotNull() & ((F.col("_val") < min_v) | (F.col("_val") > max_v))
                ).count()
                self._record("Value Range", col, bad == 0, bad)
            except Exception as e:
                self._error("Value Range", col, str(e))
        return self.results


# ── 7. AllowedValuesChecker ───────────────────────────────────────────────────

class AllowedValuesChecker(BaseChecker):
    """Validates column values belong to an approved list."""

    def __init__(self, df: DataFrame, allowed_rules: dict[str, list], dataset_name: str = "") -> None:
        super().__init__(df, dataset_name)
        self.allowed_rules = allowed_rules

    def run(self) -> list[CheckResult]:
        try:
            self._validate_columns(*self.allowed_rules.keys())
        except DQColumnError as e:
            return [self._error("Allowed Values", "multiple", str(e))]

        self._header("ALLOWED VALUES CHECK")
        for col, allowed in self.allowed_rules.items():
            self._sub_header(f"Column : {col}  |  Allowed : {allowed}")
            try:
                bad = self.df.filter(F.col(col).isNotNull() & (~F.col(col).isin(allowed))).count()
                if bad > 0:
                    self.df.filter(F.col(col).isNotNull() & (~F.col(col).isin(allowed))).select(col).distinct().show(5, truncate=False)
                self._record("Allowed Values", col, bad == 0, bad)
            except Exception as e:
                self._error("Allowed Values", col, str(e))
        return self.results


# ── 8. UniquenessChecker ──────────────────────────────────────────────────────

class UniquenessChecker(BaseChecker):
    """Checks columns have no duplicate values."""

    def __init__(self, df: DataFrame, columns: list[str], dataset_name: str = "") -> None:
        super().__init__(df, dataset_name)
        self.columns = columns

    def run(self) -> list[CheckResult]:
        try:
            self._validate_columns(*self.columns)
        except DQColumnError as e:
            return [self._error("Uniqueness", str(self.columns), str(e))]

        self._header("UNIQUENESS CHECK")
        for col in self.columns:
            try:
                unique  = self.df.select(col).distinct().count()
                dup_cnt = self.total - unique
                self._sub_header(f"Column : {col}  |  Total : {self.total}  |  Unique : {unique}  |  Dups : {dup_cnt}")
                if dup_cnt > 0:
                    self.df.groupBy(col).count().filter(F.col("count") > 1).show(5, truncate=False)
                self._record("Uniqueness", col, dup_cnt == 0, dup_cnt)
            except Exception as e:
                self._error("Uniqueness", col, str(e))
        return self.results


# ── 9. ConsistencyChecker ─────────────────────────────────────────────────────

class ConsistencyChecker(BaseChecker):
    """Validates single-table business rules via SQL expressions."""

    def __init__(self, df: DataFrame, rules: list[dict], dataset_name: str = "") -> None:
        super().__init__(df, dataset_name)
        self.rules = rules

    def run(self) -> list[CheckResult]:
        self._header("CONSISTENCY CHECK")
        for rule in self.rules:
            name, condition = rule.get("name","?"), rule.get("condition","")
            msg = rule.get("message", "")
            if not condition:
                self._skip("Consistency", name, "No condition defined")
                continue
            self._sub_header(f"Rule : {name}  |  {msg}")
            try:
                bad = self.df.filter(~F.expr(condition)).count()
                self._record("Consistency", name, bad == 0, bad)
            except Exception as e:
                self._error("Consistency", name, str(e))
        return self.results


# ── 10. ReferentialIntegrityChecker ──────────────────────────────────────────

class ReferentialIntegrityChecker(BaseChecker):
    """Checks FK values exist in a reference table."""

    def __init__(self, df: DataFrame, fk_col: str,
                 ref_df: DataFrame, ref_col: str, dataset_name: str = "") -> None:
        super().__init__(df, dataset_name)
        self.fk_col  = fk_col
        self.ref_df  = ref_df
        self.ref_col = ref_col

    def run(self) -> list[CheckResult]:
        try:
            self._validate_columns(self.fk_col)
        except DQColumnError as e:
            return [self._error("RI Check", self.fk_col, str(e))]

        self._header(f"REFERENTIAL INTEGRITY : {self.fk_col}")
        try:
            fk  = self.df.select(F.col(self.fk_col).alias("_fk")).distinct()
            ref = self.ref_df.select(F.col(self.ref_col).alias("_ref")).distinct()
            orphans = fk.join(ref, fk["_fk"] == ref["_ref"], how="left_anti").count()
            self._sub_header(f"FK : {self.fk_col}  →  Ref : {self.ref_col}  |  Orphans : {orphans}")
            self._record("Ref Integrity", self.fk_col, orphans == 0, orphans)
        except Exception as e:
            self._error("Ref Integrity", self.fk_col, str(e))
        return self.results


# ── 11. WhitespaceChecker ─────────────────────────────────────────────────────

class WhitespaceChecker(BaseChecker):
    """Detects leading/trailing whitespace in string columns."""

    def __init__(self, df: DataFrame, columns: list[str], dataset_name: str = "") -> None:
        super().__init__(df, dataset_name)
        self.columns = columns

    def run(self) -> list[CheckResult]:
        try:
            self._validate_columns(*self.columns)
        except DQColumnError as e:
            return [self._error("Whitespace", str(self.columns), str(e))]

        self._header("WHITESPACE CHECK")
        for col in self.columns:
            self._sub_header(f"Column : {col}")
            try:
                bad = self.df.filter(F.col(col).isNotNull() & (F.col(col) != F.trim(F.col(col)))).count()
                self._record("Whitespace", col, bad == 0, bad)
            except Exception as e:
                self._error("Whitespace", col, str(e))
        return self.results


# ── 12. PrecisionScaleChecker ─────────────────────────────────────────────────

class PrecisionScaleChecker(BaseChecker):
    """Checks decimal values don't exceed allowed decimal places."""

    def __init__(self, df: DataFrame, rules: dict[str, dict], dataset_name: str = "") -> None:
        super().__init__(df, dataset_name)
        self.rules = rules

    def run(self) -> list[CheckResult]:
        try:
            self._validate_columns(*self.rules.keys())
        except DQColumnError as e:
            return [self._error("Precision", "multiple", str(e))]

        self._header("PRECISION / SCALE CHECK")
        for col, rule in self.rules.items():
            scale = rule.get("scale", 2)
            self._sub_header(f"Column : {col}  |  Max scale : {scale}")
            try:
                bad = self.df.filter(
                    F.col(col).isNotNull() & F.col(col).contains(".") &
                    (F.length(F.split(F.col(col), r"\.")[1]) > scale)
                ).count()
                self._record("Precision", col, bad == 0, bad)
            except Exception as e:
                self._error("Precision", col, str(e))
        return self.results


# ── 13. CaseChecker ───────────────────────────────────────────────────────────

class CaseChecker(BaseChecker):
    """Validates string columns are in expected case (upper/lower)."""

    def __init__(self, df: DataFrame, rules: dict[str, str], dataset_name: str = "") -> None:
        super().__init__(df, dataset_name)
        self.rules = rules

    def run(self) -> list[CheckResult]:
        try:
            self._validate_columns(*self.rules.keys())
        except DQColumnError as e:
            return [self._error("Case", "multiple", str(e))]

        self._header("CASE SENSITIVITY CHECK")
        for col, case in self.rules.items():
            self._sub_header(f"Column : {col}  |  Expected : {case}")
            if case not in ("upper", "lower"):
                self._skip("Case", col, f"Unknown case '{case}'")
                continue
            try:
                bad_filter = (F.col(col) != F.upper(F.col(col))) if case == "upper" \
                             else (F.col(col) != F.lower(F.col(col)))
                bad = self.df.filter(F.col(col).isNotNull() & bad_filter).count()
                self._record("Case", col, bad == 0, bad)
            except Exception as e:
                self._error("Case", col, str(e))
        return self.results


# ── 14. SpecialCharacterChecker ───────────────────────────────────────────────

class SpecialCharacterChecker(BaseChecker):
    """Flags values containing characters outside the allowed pattern."""

    _DEFAULT = r"^[a-zA-Z0-9\s._@-]+$"

    def __init__(self, df: DataFrame, columns: list[str],
                 allowed_pattern: str = _DEFAULT, dataset_name: str = "") -> None:
        super().__init__(df, dataset_name)
        self.columns         = columns
        self.allowed_pattern = allowed_pattern

    def run(self) -> list[CheckResult]:
        try:
            self._validate_columns(*self.columns)
        except DQColumnError as e:
            return [self._error("Special Char", str(self.columns), str(e))]

        self._header("SPECIAL CHARACTER CHECK")
        for col in self.columns:
            self._sub_header(f"Column : {col}")
            try:
                bad = self.df.filter(F.col(col).isNotNull() & (~F.col(col).rlike(self.allowed_pattern))).count()
                self._record("Special Char", col, bad == 0, bad)
            except Exception as e:
                self._error("Special Char", col, str(e))
        return self.results


# ── 15. OutlierChecker ────────────────────────────────────────────────────────

class OutlierChecker(BaseChecker):
    """Detects numeric values outside mean ± (z_score × std_dev)."""

    def __init__(self, df: DataFrame, columns: list[str],
                 z_score: float = 3.0, dataset_name: str = "") -> None:
        super().__init__(df, dataset_name)
        self.columns = columns
        self.z_score = z_score

    def run(self) -> list[CheckResult]:
        try:
            self._validate_columns(*self.columns)
        except DQColumnError as e:
            return [self._error("Outlier", str(self.columns), str(e))]

        self._header(f"OUTLIER CHECK  (z={self.z_score}σ)")
        for col in self.columns:
            self._sub_header(f"Column : {col}")
            try:
                safe   = self._safe_col_name(col)
                casted = self.df.withColumn("_n", F.expr(f"try_cast(`{safe}` AS DOUBLE)"))
                stats  = casted.agg(F.mean("_n").alias("m"), F.stddev("_n").alias("s")).collect()[0]
                m, s   = stats["m"], stats["s"]
                if m is None or s is None:
                    self._skip("Outlier", col, "Cannot compute stats (all NULL?)")
                    continue
                lo, hi = m - self.z_score * s, m + self.z_score * s
                print(f"   Mean={round(m,2)}  StdDev={round(s,2)}  Range=[{round(lo,2)}, {round(hi,2)}]")
                cnt = casted.filter(F.col("_n").isNotNull() & ((F.col("_n") < lo) | (F.col("_n") > hi))).count()
                self._record("Outlier", col, cnt == 0, cnt)
            except Exception as e:
                self._error("Outlier", col, str(e))
        return self.results


# ── 16. DefaultValueChecker ───────────────────────────────────────────────────

class DefaultValueChecker(BaseChecker):
    """Flags placeholder values like N/A, null, test, dummy."""

    def __init__(self, df: DataFrame, rules: dict[str, list[str]], dataset_name: str = "") -> None:
        super().__init__(df, dataset_name)
        self.rules = rules

    def run(self) -> list[CheckResult]:
        try:
            self._validate_columns(*self.rules.keys())
        except DQColumnError as e:
            return [self._error("Default Val", "multiple", str(e))]

        self._header("DEFAULT VALUE CHECK")
        for col, defaults in self.rules.items():
            lower_defaults = [v.lower() for v in defaults]
            self._sub_header(f"Column : {col}  |  Checking : {defaults}")
            try:
                bad = self.df.filter(F.col(col).isNotNull() & F.lower(F.col(col)).isin(lower_defaults)).count()
                self._record("Default Val", col, bad == 0, bad)
            except Exception as e:
                self._error("Default Val", col, str(e))
        return self.results


# ── 17. CrossColumnChecker ────────────────────────────────────────────────────

class CrossColumnChecker(BaseChecker):
    """Validates multi-column business rules via SQL expressions."""

    def __init__(self, df: DataFrame, rules: list[dict], dataset_name: str = "") -> None:
        super().__init__(df, dataset_name)
        self.rules = rules

    def run(self) -> list[CheckResult]:
        self._header("CROSS-COLUMN VALIDATION")
        for rule in self.rules:
            name, condition = rule.get("name","?"), rule.get("condition","")
            msg = rule.get("message","")
            if not condition:
                self._skip("Cross-Column", name, "No condition defined")
                continue
            self._sub_header(f"Rule : {name}  |  {msg}")
            try:
                bad = self.df.filter(~F.expr(condition)).count()
                self._record("Cross-Column", name, bad == 0, bad)
            except Exception as e:
                self._error("Cross-Column", name, str(e))
        return self.results


# ── 18. CompletenessRatioChecker ──────────────────────────────────────────────

class CompletenessRatioChecker(BaseChecker):
    """Checks minimum fill rate (%) for columns."""

    def __init__(self, df: DataFrame, columns: list[str],
                 min_pct: float = 90.0, dataset_name: str = "") -> None:
        if not (0.0 <= min_pct <= 100.0):
            raise DQConfigError(f"min_pct must be 0–100, got {min_pct}")
        super().__init__(df, dataset_name)
        self.columns = columns
        self.min_pct = min_pct

    def run(self) -> list[CheckResult]:
        try:
            self._validate_columns(*self.columns)
        except DQColumnError as e:
            return [self._error("Completeness", str(self.columns), str(e))]

        self._header(f"COMPLETENESS RATIO CHECK  (min : {self.min_pct}%)")
        for col in self.columns:
            self._sub_header(f"Column : {col}")
            try:
                filled = self.df.filter(F.col(col).isNotNull() & (F.trim(F.col(col)) != "")).count()
                pct    = round(filled / self.total * 100, 2) if self.total > 0 else 0.0
                print(f"   Filled : {filled}/{self.total} = {pct}%  (min : {self.min_pct}%)")
                passed = pct >= self.min_pct
                self._record("Completeness", col, passed, 0 if passed else self.total - filled)
            except Exception as e:
                self._error("Completeness", col, str(e))
        return self.results


# ── 19. FreshnessChecker ──────────────────────────────────────────────────────

class FreshnessChecker(BaseChecker):
    """Checks latest date in a column is not older than max_days_old."""

    def __init__(self, df: DataFrame, date_col: str,
                 max_days_old: int = 30, date_fmt: str = "yyyy-MM-dd",
                 dataset_name: str = "") -> None:
        super().__init__(df, dataset_name)
        self.date_col     = date_col
        self.max_days_old = max_days_old
        self.date_fmt     = date_fmt

    def run(self) -> list[CheckResult]:
        try:
            self._validate_columns(self.date_col)
        except DQColumnError as e:
            return [self._error("Freshness", self.date_col, str(e))]

        self._header(f"FRESHNESS CHECK : {self.date_col}")
        try:
            latest = self.df.agg(
                F.max(F.to_date(F.col(self.date_col), self.date_fmt)).alias("latest")
            ).collect()[0]["latest"]

            self._sub_header(f"Max age allowed : {self.max_days_old} days")

            if latest is None:
                self._skip("Freshness", self.date_col, f"No valid dates. Check format '{self.date_fmt}'")
                return self.results

            days_old = (date.today() - latest).days
            print(f"   Latest : {latest}  |  Age : {days_old} days")
            self._record("Freshness", self.date_col,
                         days_old <= self.max_days_old,
                         0 if days_old <= self.max_days_old else days_old)
        except Exception as e:
            self._error("Freshness", self.date_col, str(e))
        return self.results
