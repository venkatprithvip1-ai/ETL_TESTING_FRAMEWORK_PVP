"""
reconciliation/reconcile_runner.py
====================================
ReconcileRunner — orchestrates all reconciliation checks from YAML config.
ReconcileHTMLReporter — generates HTML report for reconciliation results.
ReconcileJSONExporter — exports results to JSON.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from typing import List

import yaml
from pyspark.sql import DataFrame, SparkSession

from reconciliation.reconcile_checker import (
    AggregateMatchChecker,
    ColumnMatchChecker,
    DuplicateExplosionChecker,
    ExtraRowsChecker,
    MissingRowsChecker,
    ReconcileResult,
    RowCountChecker,
    TransformationChecker,
)


# ==============================================================================
# ReconcileRunner
# ==============================================================================

class ReconcileRunner:
    """
    Reads the reconciliation YAML config and runs all configured checks.

    Usage
    -----
    runner = ReconcileRunner(source_df, target_df, config, spark)
    results = runner.run_all()
    runner.print_summary()
    """

    def __init__(self, source_df: DataFrame, target_df: DataFrame,
                 config: dict, spark: SparkSession) -> None:
        self.src     = source_df
        self.tgt     = target_df
        self.config  = config
        self.spark   = spark
        self.log     = logging.getLogger("ReconcileRunner")
        self.results : List[ReconcileResult] = []

    def run_all(self) -> List[ReconcileResult]:
        """Runs all reconciliation checks defined in the YAML config."""
        cfg = self.config.get("checks", {})

        # ── 1. Row Count ──────────────────────────────────────────────────────
        if rc := cfg.get("row_count"):
            try:
                checker = RowCountChecker(
                    self.src, self.tgt,
                    tolerance_pct         = rc.get("tolerance_pct", 0.0),
                    expected_target_count = rc.get("expected_target_count"),
                )
                self.results.extend(checker.run())
            except Exception as exc:
                self.log.error(f"RowCountChecker failed: {exc}")

        # ── 2. Column Match ───────────────────────────────────────────────────
        if cm := cfg.get("column_match"):
            try:
                checker = ColumnMatchChecker(
                    self.src, self.tgt,
                    join_key = cm["join_key"],
                    columns  = cm["columns"],
                )
                self.results.extend(checker.run())
            except Exception as exc:
                self.log.error(f"ColumnMatchChecker failed: {exc}")

        # ── 3. Transformation ─────────────────────────────────────────────────
        if tr := cfg.get("transformation"):
            try:
                checker = TransformationChecker(
                    self.src, self.tgt,
                    join_key = tr["join_key"],
                    rules    = tr["rules"],
                )
                self.results.extend(checker.run())
            except Exception as exc:
                self.log.error(f"TransformationChecker failed: {exc}")

        # ── 4. Aggregate Match ────────────────────────────────────────────────
        if ag := cfg.get("aggregate_match"):
            try:
                checker = AggregateMatchChecker(
                    self.src, self.tgt,
                    columns       = ag["columns"],
                    functions     = ag.get("functions", ["SUM", "AVG", "MIN", "MAX"]),
                    tolerance_pct = ag.get("tolerance_pct", 0.01),
                )
                self.results.extend(checker.run())
            except Exception as exc:
                self.log.error(f"AggregateMatchChecker failed: {exc}")

        # ── 5. Missing Rows ───────────────────────────────────────────────────
        if mr := cfg.get("missing_rows"):
            try:
                checker = MissingRowsChecker(
                    self.src, self.tgt,
                    join_key = mr["join_key"],
                )
                self.results.extend(checker.run())
            except Exception as exc:
                self.log.error(f"MissingRowsChecker failed: {exc}")

        # ── 6. Extra Rows ─────────────────────────────────────────────────────
        if er := cfg.get("extra_rows"):
            try:
                checker = ExtraRowsChecker(
                    self.src, self.tgt,
                    join_key = er["join_key"],
                )
                self.results.extend(checker.run())
            except Exception as exc:
                self.log.error(f"ExtraRowsChecker failed: {exc}")

        # ── 7. Duplicate Explosion ────────────────────────────────────────────
        if de := cfg.get("duplicate_explosion"):
            try:
                checker = DuplicateExplosionChecker(
                    self.src, self.tgt,
                    join_key = de["join_key"],
                )
                self.results.extend(checker.run())
            except Exception as exc:
                self.log.error(f"DuplicateExplosionChecker failed: {exc}")

        return self.results

    def print_summary(self) -> None:
        results = self.results
        n_pass  = sum(1 for r in results if r.status == "PASS")
        n_fail  = sum(1 for r in results if r.status == "FAIL")
        n_skip  = sum(1 for r in results if r.status == "SKIP")
        n_err   = sum(1 for r in results if r.status == "ERROR")

        sep = "=" * 100
        print(f"\n{sep}")
        print(f"  RECONCILIATION SUMMARY")
        print(sep)
        print(f"  {'CHECK TYPE':<25} {'CHECK NAME':<35} {'STATUS':<8} "
              f"{'SOURCE':>14} {'TARGET':>14} {'DIFF':>12}")
        print(f"  {'-'*25} {'-'*35} {'-'*8} {'-'*14} {'-'*14} {'-'*12}")

        for r in sorted(results, key=lambda x: (x.status != "FAIL", x.check_type)):
            icon = {"PASS": "✓", "FAIL": "✗", "SKIP": "~", "ERROR": "!"}.get(r.status, "?")
            src  = r.source_value[:13] if len(r.source_value) > 13 else r.source_value
            tgt  = r.target_value[:13] if len(r.target_value) > 13 else r.target_value
            diff = r.difference[:11]   if len(r.difference)   > 11 else r.difference
            print(f"  {r.check_type:<25} {r.check_name:<35} "
                  f"{r.status} {icon:<3} {src:>14} {tgt:>14} {diff:>12}")

        print(f"\n  TOTAL={len(results)}  "
              f"✓PASS={n_pass}  ✗FAIL={n_fail}  ~SKIP={n_skip}  !ERROR={n_err}")
        print(sep)

    @property
    def fail_count(self) -> int:
        return sum(1 for r in self.results if r.status == "FAIL")


# ==============================================================================
# ReconcileHTMLReporter
# ==============================================================================

_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>ETL Reconciliation Report — {ts}</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
<style>
  :root{{--bg:#0f1117;--card:#1a1d27;--border:#2a2d3a;--text:#e2e8f0;
        --muted:#8892a4;--pass:#22c55e;--fail:#ef4444;--skip:#f59e0b;
        --error:#a855f7;--accent:#3b82f6;}}
  *{{box-sizing:border-box;margin:0;padding:0}}
  body{{background:var(--bg);color:var(--text);font-family:'Segoe UI',system-ui,sans-serif;padding:24px}}
  h1{{font-size:1.8rem;color:var(--accent);margin-bottom:4px}}
  h2{{font-size:1.1rem;color:var(--text);margin:24px 0 10px}}
  .meta{{color:var(--muted);font-size:.85rem;margin-bottom:24px}}
  .cards{{display:flex;flex-wrap:wrap;gap:14px;margin-bottom:28px}}
  .card{{background:var(--card);border:1px solid var(--border);border-radius:10px;padding:16px 22px;min-width:140px}}
  .card .num{{font-size:2rem;font-weight:700}}
  .card .lbl{{font-size:.75rem;color:var(--muted);text-transform:uppercase;letter-spacing:.05em}}
  .card.pass .num{{color:var(--pass)}}.card.fail .num{{color:var(--fail)}}
  .card.skip .num{{color:var(--skip)}}.card.total .num{{color:var(--accent)}}
  .charts{{display:flex;gap:20px;flex-wrap:wrap;margin-bottom:32px}}
  .chart-box{{background:var(--card);border:1px solid var(--border);border-radius:10px;
              padding:20px;flex:1;min-width:300px}}
  .chart-box canvas{{max-height:260px}}
  .chart-box h3{{font-size:.95rem;color:var(--muted);margin-bottom:12px}}
  table{{width:100%;border-collapse:collapse;font-size:.84rem}}
  th{{background:#12151f;color:var(--muted);text-align:left;padding:9px 12px;
      border-bottom:1px solid var(--border);text-transform:uppercase;font-size:.72rem}}
  td{{padding:9px 12px;border-bottom:1px solid var(--border)}}
  tr:last-child td{{border-bottom:none}}
  tr:hover td{{background:rgba(59,130,246,.05)}}
  .badge{{display:inline-block;border-radius:4px;padding:2px 8px;font-size:.75rem;font-weight:600}}
  .badge.PASS{{background:rgba(34,197,94,.15);color:var(--pass)}}
  .badge.FAIL{{background:rgba(239,68,68,.15);color:var(--fail)}}
  .badge.SKIP{{background:rgba(245,158,11,.15);color:var(--skip)}}
  .badge.ERROR{{background:rgba(168,85,247,.15);color:var(--error)}}
  .section{{background:var(--card);border:1px solid var(--border);border-radius:10px;
            padding:20px;margin-bottom:24px}}
  footer{{margin-top:36px;text-align:center;color:var(--muted);font-size:.78rem}}
</style>
</head>
<body>
<h1>🔄 ETL Reconciliation Report</h1>
<p class="meta">Generated: {ts} &nbsp;|&nbsp; Source: {src_file} &nbsp;→&nbsp; Target: {tgt_file}</p>

<div class="cards">
  <div class="card total"><div class="num">{total}</div><div class="lbl">Total Checks</div></div>
  <div class="card pass"><div class="num">{n_pass}</div><div class="lbl">Passed</div></div>
  <div class="card fail"><div class="num">{n_fail}</div><div class="lbl">Failed</div></div>
  <div class="card skip"><div class="num">{n_skip}</div><div class="lbl">Skipped</div></div>
</div>

<div class="charts">
  <div class="chart-box"><h3>Pass / Fail Distribution</h3><canvas id="pie"></canvas></div>
  <div class="chart-box"><h3>Checks by Type</h3><canvas id="bar"></canvas></div>
</div>

<div class="section">
<h2>Reconciliation Results</h2>
<table>
<thead><tr>
  <th>Check Type</th><th>Check Name</th><th>Status</th>
  <th>Source Value</th><th>Target Value</th><th>Difference</th><th>Message</th>
</tr></thead>
<tbody>{rows}</tbody>
</table>
</div>

<footer>ETL DQ Framework v6 — Reconciliation Module</footer>

<script>
new Chart(document.getElementById('pie'),{{
  type:'doughnut',
  data:{{labels:['PASS','FAIL','SKIP','ERROR'],
        datasets:[{{data:[{n_pass},{n_fail},{n_skip},{n_err}],
                   backgroundColor:['#22c55e','#ef4444','#f59e0b','#a855f7'],
                   borderWidth:0}}]}},
  options:{{plugins:{{legend:{{labels:{{color:'#e2e8f0'}}}}}},cutout:'60%'}}
}});

const bd={bar_json};
new Chart(document.getElementById('bar'),{{
  type:'bar',
  data:{{labels:bd.labels,datasets:[{{label:'Checks',data:bd.values,
         backgroundColor:bd.colors,borderRadius:4}}]}},
  options:{{plugins:{{legend:{{display:false}}}},
            scales:{{x:{{ticks:{{color:'#8892a4'}},grid:{{color:'#2a2d3a'}}}},
                     y:{{ticks:{{color:'#8892a4'}},grid:{{color:'#2a2d3a'}}}}}}}}
}});
</script>
</body>
</html>"""

_ROW = ("<tr><td>{check_type}</td>"
        "<td>{check_name}</td>"
        "<td><span class='badge {status}'>{status}</span></td>"
        "<td style='color:#93c5fd'>{source_value}</td>"
        "<td style='color:#93c5fd'>{target_value}</td>"
        "<td style='color:{diff_color}'>{difference}</td>"
        "<td style='color:var(--muted);font-size:.8rem'>{message}</td></tr>")


class ReconcileHTMLReporter:
    def __init__(self, results: List[ReconcileResult],
                 src_file: str = "", tgt_file: str = "") -> None:
        self.results  = results
        self.src_file = os.path.basename(src_file)
        self.tgt_file = os.path.basename(tgt_file)

    def save(self, output_path: str) -> None:
        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)

        n_pass = sum(1 for r in self.results if r.status == "PASS")
        n_fail = sum(1 for r in self.results if r.status == "FAIL")
        n_skip = sum(1 for r in self.results if r.status == "SKIP")
        n_err  = sum(1 for r in self.results if r.status == "ERROR")

        rows_html = ""
        for r in sorted(self.results, key=lambda x: (x.status != "FAIL", x.check_type)):
            rows_html += _ROW.format(
                check_type   = r.check_type,
                check_name   = r.check_name,
                status       = r.status,
                source_value = r.source_value,
                target_value = r.target_value,
                difference   = r.difference or "—",
                diff_color   = "#ef4444" if r.status == "FAIL" else "#8892a4",
                message      = r.message[:100] if r.message else "",
            )

        # Bar chart: check counts by type
        from collections import Counter
        type_counts = Counter(r.check_type for r in self.results)
        bar_data = {
            "labels": list(type_counts.keys()),
            "values": list(type_counts.values()),
            "colors": ["#ef4444" if any(r.status == "FAIL" and r.check_type == t
                                        for r in self.results) else "#22c55e"
                       for t in type_counts.keys()],
        }

        html = _HTML.format(
            ts       = datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            src_file = self.src_file,
            tgt_file = self.tgt_file,
            total    = len(self.results),
            n_pass=n_pass, n_fail=n_fail, n_skip=n_skip, n_err=n_err,
            rows     = rows_html,
            bar_json = json.dumps(bar_data),
        )

        with open(output_path, "w", encoding="utf-8") as f:
            f.write(html)
        print(f"\n  ✓ Reconciliation HTML report : {output_path}")


# ==============================================================================
# ReconcileJSONExporter
# ==============================================================================

class ReconcileJSONExporter:
    def __init__(self, results: List[ReconcileResult]) -> None:
        self.results = results

    def export(self, output_path: str) -> None:
        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
        n_pass = sum(1 for r in self.results if r.status == "PASS")
        n_fail = sum(1 for r in self.results if r.status == "FAIL")

        payload = {
            "run_timestamp" : datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "type"          : "ETL Reconciliation",
            "framework"     : "ETL DQ v6",
            "total_checks"  : len(self.results),
            "summary"       : {"PASS": n_pass, "FAIL": n_fail},
            "results"       : [r.to_dict() for r in self.results],
        }
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)
        print(f"  ✓ Reconciliation JSON export : {output_path}")
