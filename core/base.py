"""
core/base.py
============
CheckResult dataclass + BaseChecker abstract parent class.
All 19 checker classes inherit from BaseChecker.
"""

from __future__ import annotations

import logging
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from core.exceptions import DQColumnError, DQConfigError


# ==============================================================================
# CheckResult — one object per check outcome
# ==============================================================================

@dataclass
class CheckResult:
    """
    Stores the result of ONE data quality check.
    DQRunner collects ALL CheckResult objects and uses them for:
      - The final summary table printed to screen
      - The HTML report
      - The JSON export
      - Alert threshold evaluation
    """
    check_name  : str
    column      : str
    status      : str           # "PASS" | "FAIL" | "SKIP" | "ERROR"
    violations  : int  = 0
    message     : str  = ""
    dataset     : str  = ""     # which dataset this came from

    @property
    def passed(self) -> bool:
        return self.status == "PASS"

    @property
    def failed(self) -> bool:
        return self.status == "FAIL"

    def to_dict(self) -> dict:
        """Converts result to a plain dict for JSON export."""
        return {
            "dataset"    : self.dataset,
            "check_name" : self.check_name,
            "column"     : self.column,
            "status"     : self.status,
            "violations" : self.violations,
            "message"    : self.message,
        }


# ==============================================================================
# BaseChecker — abstract parent for all 19 checkers
# ==============================================================================

class BaseChecker(ABC):
    """
    Parent class that every checker inherits.

    Shared utilities:
      self.log              — logger named after subclass
      self.results          — list of CheckResult
      self.total            — cached df.count()
      _validate_columns()   — checks columns exist before Spark work
      _safe_col_name()      — prevents SQL injection in F.expr()
      _header / _sub_header — consistent output formatting
      _record / _skip / _error — save + print + log results
    """

    _SEP = "=" * 65
    _DIV = "-" * 45

    def __init__(self, df: DataFrame, dataset_name: str = "") -> None:
        self.df           = df
        self.dataset_name = dataset_name
        self.log          = logging.getLogger(self.__class__.__name__)
        self.results      : list[CheckResult] = []
        self._total       : Optional[int]     = None

    @property
    def total(self) -> int:
        """Cached df.count() — computed once, reused everywhere."""
        if self._total is None:
            self._total = self.df.count()
        return self._total

    def _validate_columns(self, *cols: str) -> None:
        """Raises DQColumnError if any column is missing."""
        existing = set(self.df.columns)
        missing  = [c for c in cols if c not in existing]
        if missing:
            raise DQColumnError(
                f"[{self.__class__.__name__}] Missing columns: {missing}. "
                f"Available: {sorted(existing)}"
            )

    @staticmethod
    def _safe_col_name(name: str) -> str:
        """Sanitizes column name for use in F.expr() — prevents SQL injection."""
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
            raise DQConfigError(
                f"Unsafe column name '{name}'. Only letters/digits/underscores allowed."
            )
        return name

    def _header(self, title: str) -> None:
        print(f"\n{self._SEP}")
        print(f"  [{self.dataset_name}]  {title}")
        print(self._SEP)
        self.log.info(f"[{self.dataset_name}] {title} started")

    def _sub_header(self, label: str) -> None:
        print(f"\n   {label}")
        print(f"   {self._DIV}")

    def _record(
        self,
        check_name : str,
        col        : str,
        passed     : bool,
        violations : int = 0,
        message    : str = "",
    ) -> CheckResult:
        """Saves, prints, and logs a single check result."""
        status = "PASS" if passed else "FAIL"
        result = CheckResult(check_name, col, status, violations, message, self.dataset_name)
        self.results.append(result)
        icon = "✓" if passed else "✗"
        if passed:
            print(f"   Result : PASS {icon}")
            self.log.info(f"[{self.dataset_name}] {check_name} PASS | col={col}")
        else:
            print(f"   Result : FAIL {icon}  —  {violations} violation(s)")
            self.log.warning(f"[{self.dataset_name}] {check_name} FAIL | col={col} | violations={violations}")
        return result

    def _skip(self, check_name: str, col: str, reason: str) -> CheckResult:
        result = CheckResult(check_name, col, "SKIP", message=reason, dataset=self.dataset_name)
        self.results.append(result)
        print(f"   Result : SKIP  ~  {reason}")
        self.log.warning(f"[{self.dataset_name}] {check_name} SKIP | col={col} | {reason}")
        return result

    def _error(self, check_name: str, col: str, reason: str) -> CheckResult:
        result = CheckResult(check_name, col, "ERROR", message=reason, dataset=self.dataset_name)
        self.results.append(result)
        print(f"   Result : ERROR !  {reason}")
        self.log.error(f"[{self.dataset_name}] {check_name} ERROR | col={col} | {reason}")
        return result

    @abstractmethod
    def run(self) -> list[CheckResult]:
        """Must be implemented by every subclass."""
        ...
