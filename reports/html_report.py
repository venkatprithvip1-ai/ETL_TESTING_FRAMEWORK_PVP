"""
reports/html_report.py
======================
Generates a self-contained HTML report with:
  - Color-coded PASS/FAIL/SKIP/ERROR table per dataset
  - Doughnut chart: overall PASS vs FAIL vs SKIP vs ERROR
  - Bar chart: violation counts per check
  - Dataset summary cards at the top
  - Dark-themed professional styling
All charts use Chart.js loaded from CDN — no extra Python packages needed.
"""

from __future__ import annotations

import json
import os
from datetime import datetime
from typing import List

from core.base import CheckResult


# ==============================================================================
# HTML Template
# ==============================================================================

_HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>ETL DQ Report — {run_timestamp}</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
<style>
  :root {{
    --bg:        #0f1117;
    --card:      #1a1d27;
    --border:    #2a2d3a;
    --text:      #e2e8f0;
    --muted:     #8892a4;
    --pass:      #22c55e;
    --fail:      #ef4444;
    --skip:      #f59e0b;
    --error:     #a855f7;
    --accent:    #3b82f6;
  }}
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ background: var(--bg); color: var(--text); font-family: 'Segoe UI', system-ui, sans-serif; padding: 24px; }}
  h1   {{ font-size: 1.8rem; color: var(--accent); margin-bottom: 4px; }}
  h2   {{ font-size: 1.2rem; color: var(--text); margin: 28px 0 12px; }}
  h3   {{ font-size: 1rem; color: var(--muted); font-weight: 500; }}
  .meta  {{ color: var(--muted); font-size: 0.85rem; margin-bottom: 28px; }}

  /* Summary cards */
  .cards {{ display: flex; flex-wrap: wrap; gap: 14px; margin-bottom: 28px; }}
  .card  {{
    background: var(--card); border: 1px solid var(--border);
    border-radius: 10px; padding: 16px 22px; min-width: 140px;
  }}
  .card .num  {{ font-size: 2rem; font-weight: 700; }}
  .card .lbl  {{ font-size: 0.75rem; color: var(--muted); text-transform: uppercase; letter-spacing: .05em; }}
  .card.pass  .num {{ color: var(--pass); }}
  .card.fail  .num {{ color: var(--fail); }}
  .card.skip  .num {{ color: var(--skip); }}
  .card.error .num {{ color: var(--error); }}
  .card.total .num {{ color: var(--accent); }}

  /* Charts row */
  .charts {{ display: flex; gap: 20px; flex-wrap: wrap; margin-bottom: 36px; }}
  .chart-box {{
    background: var(--card); border: 1px solid var(--border);
    border-radius: 10px; padding: 20px; flex: 1; min-width: 300px;
  }}
  .chart-box canvas {{ max-height: 280px; }}

  /* Dataset section */
  .dataset-block {{
    background: var(--card); border: 1px solid var(--border);
    border-radius: 10px; padding: 20px; margin-bottom: 24px;
  }}
  .ds-header {{ display: flex; align-items: center; gap: 12px; margin-bottom: 14px; }}
  .ds-badge  {{
    background: var(--accent); color: #fff;
    border-radius: 6px; padding: 3px 10px; font-size: 0.75rem; font-weight: 600;
  }}

  /* Results table */
  table {{ width: 100%; border-collapse: collapse; font-size: 0.85rem; }}
  th {{ background: #12151f; color: var(--muted); text-align: left; padding: 8px 12px;
       border-bottom: 1px solid var(--border); font-weight: 500; text-transform: uppercase; font-size: 0.72rem; }}
  td {{ padding: 8px 12px; border-bottom: 1px solid var(--border); }}
  tr:last-child td {{ border-bottom: none; }}
  tr:hover td {{ background: rgba(59,130,246,0.05); }}

  /* Status badges */
  .badge {{ display:inline-block; border-radius:4px; padding:2px 8px; font-size:0.75rem; font-weight:600; }}
  .badge.PASS  {{ background: rgba(34,197,94,0.15);  color: var(--pass);  }}
  .badge.FAIL  {{ background: rgba(239,68,68,0.15);  color: var(--fail);  }}
  .badge.SKIP  {{ background: rgba(245,158,11,0.15); color: var(--skip);  }}
  .badge.ERROR {{ background: rgba(168,85,247,0.15); color: var(--error); }}

  .violations {{ color: var(--fail); font-weight: 600; }}
  .zero       {{ color: var(--muted); }}

  footer {{ margin-top: 40px; text-align: center; color: var(--muted); font-size: 0.78rem; }}
</style>
</head>
<body>

<h1>🔍 ETL Data Quality Report</h1>
<p class="meta">Generated: {run_timestamp} &nbsp;|&nbsp; Framework: ETL DQ v6</p>

<!-- Summary Cards -->
<div class="cards">
  <div class="card total"> <div class="num">{total}</div>  <div class="lbl">Total Checks</div> </div>
  <div class="card pass">  <div class="num">{n_pass}</div> <div class="lbl">Passed</div>       </div>
  <div class="card fail">  <div class="num">{n_fail}</div> <div class="lbl">Failed</div>       </div>
  <div class="card skip">  <div class="num">{n_skip}</div> <div class="lbl">Skipped</div>      </div>
  <div class="card error"> <div class="num">{n_err}</div>  <div class="lbl">Errors</div>       </div>
</div>

<!-- Charts -->
<div class="charts">
  <div class="chart-box">
    <h3>Overall Status Distribution</h3>
    <canvas id="doughnutChart"></canvas>
  </div>
  <div class="chart-box">
    <h3>Top 15 Checks by Violations</h3>
    <canvas id="barChart"></canvas>
  </div>
</div>

<!-- Per-dataset tables -->
{dataset_sections}

<footer>ETL DQ Framework v6 &nbsp;•&nbsp; Auto-generated report &nbsp;•&nbsp; Do not edit manually</footer>

<script>
// ── Doughnut chart ────────────────────────────────────────────────────────────
new Chart(document.getElementById('doughnutChart'), {{
  type: 'doughnut',
  data: {{
    labels: ['PASS', 'FAIL', 'SKIP', 'ERROR'],
    datasets: [{{
      data: [{n_pass}, {n_fail}, {n_skip}, {n_err}],
      backgroundColor: ['#22c55e', '#ef4444', '#f59e0b', '#a855f7'],
      borderWidth: 0,
    }}]
  }},
  options: {{
    plugins: {{ legend: {{ labels: {{ color: '#e2e8f0' }} }} }},
    cutout: '65%'
  }}
}});

// ── Bar chart — top violators ─────────────────────────────────────────────────
const barData = {bar_data_json};
new Chart(document.getElementById('barChart'), {{
  type: 'bar',
  data: {{
    labels: barData.labels,
    datasets: [{{
      label: 'Violations',
      data: barData.values,
      backgroundColor: '#ef4444',
      borderRadius: 4,
    }}]
  }},
  options: {{
    indexAxis: 'y',
    plugins: {{ legend: {{ display: false }} }},
    scales: {{
      x: {{ ticks: {{ color: '#8892a4' }}, grid: {{ color: '#2a2d3a' }} }},
      y: {{ ticks: {{ color: '#e2e8f0', font: {{ size: 11 }} }}, grid: {{ display: false }} }}
    }}
  }}
}});
</script>
</body>
</html>
"""

_DATASET_SECTION = """
<div class="dataset-block">
  <div class="ds-header">
    <span class="ds-badge">DATASET</span>
    <h2>{dataset_name}</h2>
    <span style="color:var(--muted); font-size:0.82rem; margin-left:auto;">
      ✓{n_pass} &nbsp; ✗{n_fail} &nbsp; ~{n_skip} &nbsp; !{n_err}
    </span>
  </div>
  <table>
    <thead>
      <tr>
        <th>Check Type</th>
        <th>Column / Rule</th>
        <th>Status</th>
        <th style="text-align:right">Violations</th>
        <th>Message</th>
      </tr>
    </thead>
    <tbody>
      {rows}
    </tbody>
  </table>
</div>
"""

_ROW = """<tr>
  <td>{check_name}</td>
  <td><code style="color:#93c5fd">{column}</code></td>
  <td><span class="badge {status}">{status}</span></td>
  <td style="text-align:right">
    <span class="{vio_class}">{violations}</span>
  </td>
  <td style="color:var(--muted); font-size:0.8rem">{message}</td>
</tr>"""


# ==============================================================================
# HTMLReporter
# ==============================================================================

class HTMLReporter:
    """
    Generates a self-contained HTML report from a list of CheckResult objects.

    Usage
    -----
    reporter = HTMLReporter(all_results)
    reporter.save("reports/dq_report_2026-03-06.html")
    """

    def __init__(self, all_results: List[CheckResult]) -> None:
        self.results = all_results

    def _dataset_section(self, dataset_name: str) -> str:
        """Generates the HTML block for one dataset's results table."""
        results = [r for r in self.results if r.dataset == dataset_name]
        # Sort: FAIL first, then SKIP, then PASS — so problems are visible immediately
        results.sort(key=lambda r: {"FAIL": 0, "ERROR": 1, "SKIP": 2, "PASS": 3}.get(r.status, 4))

        rows = "\n".join(
            _ROW.format(
                check_name = r.check_name,
                column     = r.column,
                status     = r.status,
                violations = f"{r.violations:,}" if r.violations > 0 else "—",
                vio_class  = "violations" if r.violations > 0 else "zero",
                message    = r.message[:80] if r.message else "",
            )
            for r in results
        )

        return _DATASET_SECTION.format(
            dataset_name = dataset_name,
            n_pass  = sum(1 for r in results if r.status == "PASS"),
            n_fail  = sum(1 for r in results if r.status == "FAIL"),
            n_skip  = sum(1 for r in results if r.status == "SKIP"),
            n_err   = sum(1 for r in results if r.status == "ERROR"),
            rows    = rows,
        )

    def _bar_chart_data(self) -> str:
        """Builds the top-15 violators data for the bar chart."""
        # Collect only FAIL results with violations > 0
        fail_results = [r for r in self.results if r.status == "FAIL" and r.violations > 0]
        # Sort by violations descending, take top 15
        fail_results.sort(key=lambda r: r.violations, reverse=True)
        top = fail_results[:15]

        labels = [f"{r.dataset[:8]}:{r.check_name[:14]}" for r in top]
        values = [r.violations for r in top]
        return json.dumps({"labels": labels, "values": values})

    def build(self) -> str:
        """Builds and returns the complete HTML string."""
        datasets = sorted(set(r.dataset for r in self.results))

        dataset_sections = "\n".join(self._dataset_section(ds) for ds in datasets)

        n_pass = sum(1 for r in self.results if r.status == "PASS")
        n_fail = sum(1 for r in self.results if r.status == "FAIL")
        n_skip = sum(1 for r in self.results if r.status == "SKIP")
        n_err  = sum(1 for r in self.results if r.status == "ERROR")

        return _HTML_TEMPLATE.format(
            run_timestamp    = datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            total            = len(self.results),
            n_pass           = n_pass,
            n_fail           = n_fail,
            n_skip           = n_skip,
            n_err            = n_err,
            dataset_sections = dataset_sections,
            bar_data_json    = self._bar_chart_data(),
        )

    def save(self, output_path: str) -> None:
        """Saves the HTML report to the given path."""
        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(self.build())
        print(f"\n  ✓ HTML report saved : {output_path}")
