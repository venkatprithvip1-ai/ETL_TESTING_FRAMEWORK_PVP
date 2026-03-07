"""
main_v6.py
==========
ETL Data Quality Framework — Version 6
Supports two modes:

  Mode 1 — DQ Checks (original):
    python main_v6.py --dataset students

  Mode 2 — Reconciliation (new):
    python main_v6.py --reconcile --dataset students

  Run both on same dataset:
    python main_v6.py --dataset students --reconcile
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from core.exceptions import DQConfigError, DQDataReadError, DQSparkError
from reports.html_report import HTMLReporter
from reports.json_export import JSONExporter
from runner.dq_runner import DataReader, DQConfigLoader, DQRunner, SparkSessionManager


# ==============================================================================
# LOGGING SETUP
# ==============================================================================

def _setup_logging() -> str:
    os.makedirs("logs", exist_ok=True)
    log_file = f"logs/dq_run_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.log"
    logging.basicConfig(
        level    = logging.DEBUG,
        format   = "%(asctime)s | %(levelname)-8s | %(name)-30s | %(message)s",
        datefmt  = "%Y-%m-%d %H:%M:%S",
        handlers = [logging.FileHandler(log_file)],
    )
    logging.getLogger("py4j").setLevel(logging.ERROR)
    logging.getLogger("pyspark").setLevel(logging.ERROR)
    return log_file


LOG_FILE = _setup_logging()


# ==============================================================================
# Dataset registry
# ==============================================================================

DATASETS = [
   
    #{"name": "employees",    "config": "config/rules_employees.yaml"},
    #{"name": "orders",       "config": "config/rules_orders.yaml"},
    #{"name": "products",     "config": "config/rules_products.yaml"},
    #{"name": "transactions", "config": "config/rules_transactions.yaml"},
    #{"name": "customers",    "config": "config/rules_customers.yaml"},
    
    {"name": "students",     "config": "config/rules_students.yaml"},
]

# Reconciliation config registry — one entry per dataset that has a recon config
RECON_DATASETS = [
    {"name": "students", "config": "config/reconcile_students.yaml"},
    # Add more here as you create reconciliation configs for other datasets:
    # {"name": "employees", "config": "config/reconcile_employees.yaml"},
]


# ==============================================================================
# DQ Checks mode
# ==============================================================================

def run_dq_checks(spark, datasets_to_run: list, generate_report: bool) -> None:
    log     = logging.getLogger("DQ_CHECKS")
    reader  = DataReader(spark)
    all_results = []

    for ds_info in datasets_to_run:
        name        = ds_info["name"]
        config_path = ds_info["config"]

        print(f"\n{'─'*90}")
        print(f"  DATASET: {name.upper()}")
        print(f"{'─'*90}")

        try:
            config = DQConfigLoader(config_path).load()
            file_path = config["dataset"]["file"]
            fmt       = config["dataset"].get("format", "csv")
            df        = reader.read(file_path, fmt)
            print(f"  Rows: {df.count():,}  |  Columns: {len(df.columns)}")

            runner = DQRunner(df=df, spark=spark, config=config,
                              dataset_name=name, max_workers=1)
            results = runner.run_all()
            runner.print_summary()
            all_results.extend(results)

            # Alerts
            from alerts.alert_manager import AlertManager
            alert_cfg = config.get("alerts", {})
            AlertManager(alert_cfg).fire_if_needed(name, runner.fail_count, results)

        except DQConfigError as exc:
            print(f"\n  [CONFIG ERROR] {name}: {exc}")
        except DQDataReadError as exc:
            print(f"\n  [DATA ERROR] {name}: {exc}")
        except Exception as exc:
            print(f"\n  [ERROR] {name}: {exc}")
            log.error(f"Error for {name}: {exc}", exc_info=True)

    if all_results:
        ts        = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        json_path = f"reports/dq_results_{ts}.json"
        JSONExporter(all_results).export(json_path)

        if generate_report:
            html_path = f"reports/dq_report_{ts}.html"
            HTMLReporter(all_results).save(html_path)
            print(f"\n  Open in browser: file://{os.path.abspath(html_path)}")

    return all_results


# ==============================================================================
# Reconciliation mode  ← NEW
# ==============================================================================

def run_reconciliation(spark, datasets_to_run: list, generate_report: bool) -> None:
    """
    Runs Source vs Target reconciliation checks.
    Reads reconcile_*.yaml config files from config/ folder.
    """
    log    = logging.getLogger("RECONCILE")
    reader = DataReader(spark)

    # Filter RECON_DATASETS to only the requested ones
    recon_to_run = [
        d for d in RECON_DATASETS
        if any(d["name"] == ds["name"] for ds in datasets_to_run)
    ]

    if not recon_to_run:
        print("\n  No reconciliation configs found for the requested datasets.")
        print("  Make sure config/reconcile_<dataset>.yaml exists.")
        return

    for ds_info in recon_to_run:
        name        = ds_info["name"]
        config_path = ds_info["config"]

        print(f"\n{'═'*90}")
        print(f"  RECONCILIATION: {name.upper()}")
        print(f"{'═'*90}")

        try:
            # Load reconciliation YAML
            if not os.path.exists(config_path):
                print(f"\n  [SKIP] Config not found: {config_path}")
                continue

            import yaml
            with open(config_path, "r") as f:
                config = yaml.safe_load(f)

            recon_cfg  = config.get("reconciliation", {})
            src_path   = recon_cfg.get("source_file", "")
            tgt_path   = recon_cfg.get("target_file", "")
            fmt        = recon_cfg.get("format", "csv")

            # Load source and target DataFrames
            print(f"\n  Loading source : {src_path}")
            source_df = reader.read(src_path, fmt)
            print(f"  Source rows    : {source_df.count():,}")

            print(f"  Loading target : {tgt_path}")
            target_df = reader.read(tgt_path, fmt)
            print(f"  Target rows    : {target_df.count():,}")

            # Run all reconciliation checks
            from reconciliation.reconcile_runner import (
                ReconcileRunner, ReconcileHTMLReporter, ReconcileJSONExporter
            )

            runner = ReconcileRunner(source_df, target_df, config, spark)
            results = runner.run_all()
            runner.print_summary()

            # Save outputs
            ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
            os.makedirs("reports", exist_ok=True)

            json_path = f"reports/recon_results_{name}_{ts}.json"
            ReconcileJSONExporter(results).export(json_path)

            if generate_report:
                html_path = f"reports/recon_report_{name}_{ts}.html"
                ReconcileHTMLReporter(results, src_path, tgt_path).save(html_path)
                print(f"  Open in browser: file://{os.path.abspath(html_path)}")

        except DQDataReadError as exc:
            print(f"\n  [DATA ERROR] {name}: {exc}")
            print("  Make sure you ran generate_student_transformed.py first.")
        except Exception as exc:
            print(f"\n  [ERROR] {name}: {exc}")
            log.error(f"Reconciliation error for {name}: {exc}", exc_info=True)


# ==============================================================================
# main
# ==============================================================================

def main(run_datasets     : list | None = None,
         generate_report  : bool        = True,
         reconcile_mode   : bool        = False) -> None:

    log = logging.getLogger("MAIN")
    log.info("ETL DQ FRAMEWORK v6 STARTED")

    print("\n" + "=" * 90)
    print("  ETL DATA QUALITY FRAMEWORK — VERSION 6")
    mode_label = "Reconciliation Mode" if reconcile_mode else "DQ Check Mode"
    print(f"  {mode_label} | YAML Config | HTML Report | Alerts")
    print("=" * 90)

    session_manager = None

    try:
        session_manager = SparkSessionManager("ETL_DQ_v6")
        spark           = session_manager.start()

        # Determine which datasets to run
        datasets_to_run = [
            d for d in DATASETS
            if run_datasets is None or d["name"] in run_datasets
        ]

        if not datasets_to_run:
            print(f"\n  No matching datasets for: {run_datasets}")
            return

        print(f"\n  Mode      : {mode_label}")
        print(f"  Datasets  : {[d['name'] for d in datasets_to_run]}")

        if reconcile_mode:
            # ── RECONCILIATION MODE ───────────────────────────────────────────
            run_reconciliation(spark, datasets_to_run, generate_report)
        else:
            # ── DQ CHECK MODE ─────────────────────────────────────────────────
            all_results = run_dq_checks(spark, datasets_to_run, generate_report)

            # Grand total
            if all_results:
                n_pass = sum(1 for r in all_results if r.status == "PASS")
                n_fail = sum(1 for r in all_results if r.status == "FAIL")
                n_skip = sum(1 for r in all_results if r.status == "SKIP")
                n_err  = sum(1 for r in all_results if r.status == "ERROR")
                print(f"\n{'='*90}")
                print(f"  GRAND TOTAL ACROSS ALL DATASETS")
                print(f"  Total={len(all_results)}  ✓PASS={n_pass}  ✗FAIL={n_fail}  "
                      f"~SKIP={n_skip}  !ERROR={n_err}")
                print(f"  Log  : {LOG_FILE}")
                print(f"{'='*90}\n")

    except DQSparkError as exc:
        print(f"\n  [SPARK ERROR] {exc}")
    except KeyboardInterrupt:
        print("\n  Run cancelled.")
    finally:
        if session_manager:
            session_manager.stop()

    log.info("ETL DQ FRAMEWORK v6 FINISHED")


# ==============================================================================
# CLI
# ==============================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL DQ Framework v6")
    parser.add_argument(
        "--dataset", nargs="*", default=None,
        help="Dataset(s) to run. Default: all. Example: --dataset students"
    )
    parser.add_argument(
        "--reconcile", action="store_true", default=False,
        help="Run SOURCE vs TARGET reconciliation instead of DQ checks"
    )
    parser.add_argument(
        "--no-report", action="store_true", default=False,
        help="Skip HTML report generation"
    )
    args = parser.parse_args()

    main(
        run_datasets    = args.dataset,
        generate_report = not args.no_report,
        reconcile_mode  = args.reconcile,
    )
