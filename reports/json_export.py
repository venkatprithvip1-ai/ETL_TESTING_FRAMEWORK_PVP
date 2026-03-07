"""
reports/json_export.py
======================
Exports all CheckResult objects to a structured JSON file.
Useful for CI/CD pipelines, dashboards, and audit logs.
"""

from __future__ import annotations

import json
import os
from datetime import datetime
from typing import List

from core.base import CheckResult


class JSONExporter:
    """
    Exports DQ results to a JSON file.

    Output format:
    {
      "run_timestamp"   : "2026-03-06 14:30:00",
      "framework"       : "ETL DQ v6",
      "total_checks"    : 120,
      "summary"         : { "PASS": 95, "FAIL": 20, "SKIP": 3, "ERROR": 2 },
      "results"         : [ { ... }, { ... } ]
    }
    """

    def __init__(self, all_results: List[CheckResult]) -> None:
        self.results = all_results

    def export(self, output_path: str) -> None:
        """Saves results to a JSON file."""
        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)

        status_counts = {"PASS": 0, "FAIL": 0, "SKIP": 0, "ERROR": 0}
        for r in self.results:
            status_counts[r.status] = status_counts.get(r.status, 0) + 1

        payload = {
            "run_timestamp" : datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "framework"     : "ETL DQ v6",
            "total_checks"  : len(self.results),
            "summary"       : status_counts,
            "results"       : [r.to_dict() for r in self.results],
        }

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)

        print(f"  ✓ JSON export saved : {output_path}")
