# ETL DQ Framework v6 — Complete Setup Guide

---

## PROJECT STRUCTURE

```
etl_dq_v6/
│
├── main_v6.py                    ← Entry point — run this
│
├── config/
│   ├── rules_employees.yaml      ← DQ rules for employees dataset
│   ├── rules_orders.yaml         ← DQ rules for orders
│   ├── rules_products.yaml       ← DQ rules for products
│   ├── rules_transactions.yaml   ← DQ rules for transactions
│   └── rules_customers.yaml      ← DQ rules for customers
│
├── core/
│   ├── exceptions.py             ← Custom exception classes
│   └── base.py                   ← BaseChecker + CheckResult
│
├── checkers/
│   └── all_checkers.py           ← All 19 DQ checker classes
│
├── runner/
│   └── dq_runner.py              ← SparkSessionManager, DataReader,
│                                    DQConfigLoader, DQRunner (parallel)
│
├── reports/
│   ├── html_report.py            ← HTML report with charts
│   └── json_export.py            ← JSON export
│
├── alerts/
│   └── alert_manager.py          ← Slack + Email alerts
│
├── data/
│   ├── generate_sample_data.py   ← Generates 5 large CSV datasets
│   ├── employees.csv             ← generated (100,000 rows)
│   ├── orders.csv                ← generated (200,000 rows)
│   ├── products.csv              ← generated (50,000 rows)
│   ├── transactions.csv          ← generated (500,000 rows)
│   └── customers.csv             ← generated (150,000 rows)
│
├── tests/
│   └── test_dq_checks.py         ← pytest unit tests
│
├── .github/
│   └── workflows/
│       └── dq_pipeline.yml       ← GitHub Actions CI/CD
│
├── logs/                         ← auto-created at runtime
│   └── dq_run_YYYY-MM-DD.log
│
├── reports/                      ← auto-created at runtime
│   ├── dq_report_YYYY-MM-DD.html
│   └── dq_results_YYYY-MM-DD.json
│
└── requirements.txt
```

---

## STEP-BY-STEP SETUP IN A FRESH VENV

### Step 1 — Create the project folder

```bash
mkdir etl_dq_v6
cd etl_dq_v6
```

### Step 2 — Create a fresh virtual environment

```bash
# On Mac / Linux:
python3.11 -m venv venv

# On Windows:
python -m venv venv
```

### Step 3 — Activate the virtual environment

```bash
# Mac / Linux:
source venv/bin/activate

# Windows:
venv\Scripts\activate
```

You should see `(venv)` at the start of your terminal prompt.

### Step 4 — Verify you are using the VENV Python (not system Python)

```bash
which python
# Should show: /path/to/etl_dq_v6/venv/bin/python
# NOT: /opt/homebrew/bin/python3.11
```

### Step 5 — Install all dependencies

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

This installs: pyspark, pyyaml, pytest, pytest-cov

Verify installation:
```bash
python -c "import pyspark; print('PySpark:', pyspark.__version__)"
python -c "import yaml;    print('PyYAML OK')"
```

### Step 6 — Java check (PySpark requires Java 8, 11, or 17)

```bash
java -version
# Should show version 8, 11, or 17
```

If Java is missing on Mac:
```bash
brew install openjdk@11
export JAVA_HOME=/opt/homebrew/opt/openjdk@11
```

Add this to your ~/.zshrc or ~/.bashrc to make it permanent:
```bash
echo 'export JAVA_HOME=/opt/homebrew/opt/openjdk@11' >> ~/.zshrc
source ~/.zshrc
```

### Step 7 — Generate sample data (1,000,000 rows across 5 files)

```bash
python data/generate_sample_data.py
```

Expected output:
```
  Generating employees.csv    (100,000 rows)...  ✓
  Generating orders.csv       (200,000 rows)...  ✓
  Generating products.csv      (50,000 rows)...  ✓
  Generating transactions.csv (500,000 rows)...  ✓
  Generating customers.csv    (150,000 rows)...  ✓
```

### Step 8 — Run all DQ checks

```bash
python main_v6.py
```

### Step 9 — View the HTML report

After the run, open the report in your browser:
```bash
open reports/dq_report_*.html        # Mac
xdg-open reports/dq_report_*.html   # Linux
start reports/dq_report_*.html      # Windows
```

---

## COMMON COMMANDS

```bash
# Run all 5 datasets:
python main_v6.py

# Run only employees and orders:
python main_v6.py --dataset employees orders

# Run without generating HTML report (faster):
python main_v6.py --no-report

# Run unit tests:
pytest tests/ -v

# Run tests with coverage report:
pytest tests/ -v --cov=checkers --cov=core --cov-report=html
open htmlcov/index.html   # view coverage

# Run one specific test class:
pytest tests/test_dq_checks.py::TestPrimaryKeyChecker -v
```

---

## HOW TO ADD A NEW DATASET

### 1 — Add your CSV file to data/

```bash
cp /path/to/yourfile.csv data/yourfile.csv
```

### 2 — Create a YAML config

Copy and edit an existing one:
```bash
cp config/rules_employees.yaml config/rules_yourfile.yaml
```

Edit the dataset section and checks to match your columns.

### 3 — Register it in main_v6.py

Open main_v6.py and add one line to the DATASETS list:
```python
DATASETS = [
    {"name": "employees",    "config": "config/rules_employees.yaml"},
    # ... existing entries ...
    {"name": "yourfile",     "config": "config/rules_yourfile.yaml"},  # ← add this
]
```

### 4 — Run it

```bash
python main_v6.py --dataset yourfile
```

---

## HOW TO CONFIGURE ALERTS

### Slack Alert

1. Go to https://api.slack.com/apps
2. Create App → Incoming Webhooks → Add to Workspace
3. Copy the webhook URL
4. Edit your YAML config:

```yaml
alerts:
  fail_threshold: 5          # alert if more than 5 checks fail
  slack:
    enabled: true
    webhook_url: "https://hooks.slack.com/services/YOUR/URL/HERE"
```

### Email Alert (Gmail)

1. Enable 2-step verification on your Google account
2. Go to: Google Account → Security → App Passwords → Create
3. Set environment variable:
   ```bash
   export DQ_EMAIL_PASSWORD="your_16_char_app_password"
   ```
4. Edit your YAML config:

```yaml
alerts:
  fail_threshold: 3
  email:
    enabled: true
    smtp_host:  smtp.gmail.com
    smtp_port:  587
    sender:     your_email@gmail.com
    password:   ""           # leave blank — read from DQ_EMAIL_PASSWORD env var
    recipients: [team@company.com, manager@company.com]
```

---

## HOW TO ADD A PARQUET OR DELTA LAKE SOURCE

Change the format in your YAML:
```yaml
dataset:
  name: my_dataset
  file: data/my_data.parquet   # or /path/to/delta/table
  format: parquet              # or "delta"
```

For Delta Lake, also install the delta-spark package:
```bash
pip install delta-spark
```

---

## TROUBLESHOOTING

| Problem | Fix |
|---------|-----|
| `ModuleNotFoundError: No module named 'pyspark'` | Use `python` not `/opt/homebrew/bin/python3.11`. Check `which python`. |
| `JAVA_HOME not set` | Run `export JAVA_HOME=$(/usr/libexec/java_home)` on Mac |
| `File not found: data/employees.csv` | Run `python data/generate_sample_data.py` first |
| `ModuleNotFoundError: No module named 'yaml'` | Run `pip install pyyaml` |
| Tests fail with `SparkContext already stopped` | Each test class shares the spark fixture. This is expected. |
| Slow performance on large datasets | Increase Spark partitions in `dq_runner.py`: `.config("spark.sql.shuffle.partitions", "16")` |
| HTML report not opening | Try `python -m http.server 8080` in the reports folder and open `http://localhost:8080` |

---

## ALWAYS RUN PYTHON USING THE VENV

**Wrong** (uses system Python, not your venv):
```bash
/opt/homebrew/bin/python3.11 main_v6.py
```

**Correct** (uses venv Python with all packages installed):
```bash
source venv/bin/activate   # activate first
python main_v6.py          # then run
```
